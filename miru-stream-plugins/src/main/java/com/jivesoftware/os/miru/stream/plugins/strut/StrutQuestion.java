package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsDebug;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Question;
import com.jivesoftware.os.miru.stream.plugins.strut.Strut.Scored;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author jonathan
 */
public class StrutQuestion implements Question<StrutQuery, StrutAnswer, StrutReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ExecutorService asyncRescorers;
    private final Strut strut;
    private final MiruRequest<StrutQuery> request;
    private final MiruRemotePartition<StrutQuery, StrutAnswer, StrutReport> remotePartition;
    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public StrutQuestion(ExecutorService asyncRescorers,
        Strut strut,
        MiruRequest<StrutQuery> request,
        MiruRemotePartition<StrutQuery, StrutAnswer, StrutReport> remotePartition) {
        this.asyncRescorers = asyncRescorers;
        this.strut = strut;
        this.request = request;
        this.remotePartition = remotePartition;
    }

    @Override
    public <BM extends IBM, IBM> MiruPartitionResponse<StrutAnswer> askLocal(MiruRequestHandle<BM, IBM, ?> handle,
        Optional<StrutReport> report)
        throws Exception {

        StackBuffer stackBuffer = new StackBuffer();
        MiruSolutionLog solutionLog = new MiruSolutionLog(request.logLevel);
        MiruRequestContext<BM, IBM, ?> context = handle.getRequestContext();
        MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();

        MiruTimeRange timeRange = request.query.timeRange;
        if (!context.getTimeIndex().intersects(timeRange)) {
            solutionLog.log(MiruSolutionLogLevel.WARN, "No time index intersection. Partition {}: {} doesn't intersect with {}",
                handle.getCoord().partitionId, context.getTimeIndex(), timeRange);
            StrutAnswer answer = strut.composeAnswer(context, request, Collections.emptyList());
            return new MiruPartitionResponse<>(answer, solutionLog.asList());
        }

        int activityIndexLastId = context.getActivityIndex().lastId(stackBuffer);

        List<IBM> ands = new ArrayList<>();
        ands.add(bitmaps.buildIndexMask(activityIndexLastId, context.getRemovalIndex().getIndex(stackBuffer)));
        MiruSchema schema = context.getSchema();
        MiruTermComposer termComposer = context.getTermComposer();

        if (!MiruFilter.NO_FILTER.equals(request.query.constraintFilter)) {
            BM constrained = aggregateUtil.filter("strutGather",
                bitmaps, schema, termComposer,
                context.getFieldIndexProvider(),
                request.query.constraintFilter,
                solutionLog,
                null,
                activityIndexLastId,
                -1,
                stackBuffer);
            ands.add(constrained);
        }

        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            ands.add(context.getAuthzIndex().getCompositeAuthz(request.authzExpression, stackBuffer));
        }

        if (!MiruTimeRange.ALL_TIME.equals(request.query.timeRange)) {
            ands.add(bitmaps.buildTimeRangeMask(context.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp, stackBuffer));
        }

        ands.add(bitmaps.buildIndexMask(activityIndexLastId, context.getRemovalIndex().getIndex(stackBuffer)));

        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        BM eligible = bitmaps.and(ands);

        int pivotFieldId = schema.getFieldId(request.query.constraintField);

        MiruFieldIndex<BM, IBM> primaryIndex = context.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);

        MinMaxPriorityQueue<Scored> scored = MinMaxPriorityQueue
            .expectedSize(request.query.desiredNumberOfResults)
            .maximumSize(request.query.desiredNumberOfResults)
            .create();

        MiruPluginCacheProvider.CacheKeyValues cacheStores = handle.getRequestContext()
            .getCacheProvider()
            .get("strut-" + request.query.catwalkId, 8, false);

        StrutModelScorer modelScorer = new StrutModelScorer(); // Ahh

        long start = System.currentTimeMillis();
        List<LastIdAndTermId> lastIdAndTermIds = Lists.newArrayList();
        //TODO config batch size
        aggregateUtil.gather("strut", bitmaps, context, eligible, pivotFieldId, 100, true, solutionLog, (lastId, termId) -> {
            lastIdAndTermIds.add(new LastIdAndTermId(lastId, termId));
            return true;
        }, stackBuffer);

        solutionLog.log(MiruSolutionLogLevel.INFO, "Strut accumulated {} terms in {} ms", lastIdAndTermIds.size(), System.currentTimeMillis() - start);
        start = System.currentTimeMillis();

        long totalTimeFetchingLastId = 0;
        long totalTimeFetchingScores = 0;

        int batchSize = 100; //TODO config batch size
        BM[] answers = bitmaps.createArrayOf(batchSize);
        int[] scoredToLastIds = new int[batchSize];
        MiruTermId[] nullableMiruTermIds = new MiruTermId[batchSize];
        MiruTermId[] miruTermIds = new MiruTermId[batchSize];
        List<MiruTermId> asyncRescore = Lists.newArrayList();
        int totalMisses = 0;
        done:
        for (List<LastIdAndTermId> batch : Lists.partition(lastIdAndTermIds, answers.length)) {

            Arrays.fill(miruTermIds, null);
            for (int i = 0; i < batch.size(); i++) {
                miruTermIds[i] = batch.get(i).termId;
            }

            System.arraycopy(miruTermIds, 0, nullableMiruTermIds, 0, batchSize);
            Arrays.fill(scoredToLastIds, -1);

            long fetchLastIdsStart = System.currentTimeMillis();
            primaryIndex.multiGetLastIds("strut", pivotFieldId, nullableMiruTermIds, scoredToLastIds, stackBuffer);
            totalTimeFetchingLastId += (System.currentTimeMillis() - fetchLastIdsStart);

            if (request.query.usePartitionModelCache) {
                long fetchScoresStart = System.currentTimeMillis();
                modelScorer.score(
                    request.query.modelId,
                    miruTermIds,
                    cacheStores,
                    (termIndex, score, scoredToLastId) -> {
                        if (Float.isNaN(score) || scoredToLastId < scoredToLastIds[termIndex]) {
                            asyncRescore.add(miruTermIds[termIndex]);
                        }
                        scored.add(new Scored(batch.get(termIndex).lastId,
                            miruTermIds[termIndex],
                            scoredToLastIds[termIndex],
                            Float.isNaN(score) ? 0f : score,
                            -1,
                            null));
                        return true;
                    },
                    stackBuffer);
                totalTimeFetchingScores += (System.currentTimeMillis() - fetchScoresStart);
            } else {
                List<Scored> rescored = rescore(handle, Arrays.asList(miruTermIds), pivotFieldId, modelScorer, cacheStores);
                scored.addAll(rescored);
            }

        }

        solutionLog.log(MiruSolutionLogLevel.INFO, "Struted our stuff for {} which took lastIds {} ms cached {} ms total {} ms total missed {}",
            lastIdAndTermIds.size(),
            totalTimeFetchingLastId,
            totalTimeFetchingScores,
            System.currentTimeMillis() - start,
            totalMisses);

        if (!asyncRescore.isEmpty()) {
            asyncRescorers.submit(() -> {
                try {
                    rescore(handle, asyncRescore, pivotFieldId, modelScorer, cacheStores);
                } catch (Exception x) {
                    LOG.warn("Failed while trying to rescore.", x);
                }
            });
        }

        int[] gatherFieldIds = null;
        if (request.query.gatherTermsForFields != null && request.query.gatherTermsForFields.length > 0) {
            gatherFieldIds = new int[request.query.gatherTermsForFields.length];
            for (int i = 0; i < gatherFieldIds.length; i++) {
                gatherFieldIds[i] = schema.getFieldId(request.query.gatherTermsForFields[i]);
                Preconditions.checkArgument(schema.getFieldDefinition(gatherFieldIds[i]).type.hasFeature(MiruFieldDefinition.Feature.stored),
                    "You can only gather stored fields");
            }
        }

        MiruActivityIndex activityIndex = context.getActivityIndex();
        MiruFieldDefinition pivotFieldDefinition = schema.getFieldDefinition(pivotFieldId);
        List<HotOrNot> hotOrNots = new ArrayList<>(request.query.desiredNumberOfResults);

        Scored[] s = scored.toArray(new Scored[0]);
        int[] scoredLastIds = new int[s.length];
        for (int j = 0; j < s.length; j++) {
            scoredLastIds[j] = s[j].lastId;
        }

        MiruValue[][][] gatherScoredValues = null;
        if (gatherFieldIds != null) {
            gatherScoredValues = new MiruValue[scoredLastIds.length][gatherFieldIds.length][];
            int[] consumeLastIds = new int[scoredLastIds.length];
            for (int j = 0; j < gatherFieldIds.length; j++) {
                System.arraycopy(scoredLastIds, 0, consumeLastIds, 0, scoredLastIds.length);
                MiruTermId[][] termIds = activityIndex.getAll("strut", consumeLastIds, gatherFieldIds[j], stackBuffer);
                for (int k = 0; k < termIds.length; k++) {
                    if (termIds[k] != null) {
                        gatherScoredValues[k][j] = new MiruValue[termIds[k].length];
                        for (int l = 0; l < termIds[k].length; l++) {
                            gatherScoredValues[k][j][l] = new MiruValue(termComposer.decompose(schema,
                                schema.getFieldDefinition(gatherFieldIds[j]),
                                stackBuffer,
                                termIds[k][l]));
                        }
                    }
                }
            }
        }

        int[] consumeLastIds = new int[scoredLastIds.length];
        System.arraycopy(scoredLastIds, 0, consumeLastIds, 0, scoredLastIds.length);
        TimeAndVersion[] timeAndVersions = activityIndex.getAllTimeAndVersions("strut", consumeLastIds, stackBuffer);

        for (int j = 0; j < s.length; j++) {
            if (timeAndVersions[j] != null) {
                String[] decomposed = termComposer.decompose(schema, pivotFieldDefinition, stackBuffer, s[j].term);
                hotOrNots.add(new HotOrNot(new MiruValue(decomposed),
                    gatherScoredValues != null ? gatherScoredValues[j] : null,
                    s[j].score, s[j].termCount, s[j].features,
                    timeAndVersions[j].timestamp));
            } else {
                LOG.warn("Failed to get timestamp for {}", scoredLastIds[j]);
            }
        }
        solutionLog.log(MiruSolutionLogLevel.INFO, "Strut found {} terms", hotOrNots.size());

        return new MiruPartitionResponse<>(strut.composeAnswer(context, request, hotOrNots), solutionLog.asList());
    }

    private <BM extends IBM, IBM> List<Scored> rescore(
        MiruRequestHandle<BM, IBM, ?> handle,
        List<MiruTermId> score,
        int pivotFieldId,
        StrutModelScorer modelScorer,
        MiruPluginCacheProvider.CacheKeyValues cacheStores) throws Exception {

        MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();
        MiruRequestContext<BM, IBM, ?> context = handle.getRequestContext();
        MiruFieldIndex<BM, IBM> primaryIndex = context.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);

        MiruSchema schema = context.getSchema();
        MiruTermComposer termComposer = context.getTermComposer();

        MiruSolutionLog solutionLog = new MiruSolutionLog(MiruSolutionLogLevel.NONE);
        StackBuffer stackBuffer = new StackBuffer();

        int activityIndexLastId = context.getActivityIndex().lastId(stackBuffer);

        BM constrainFeature;
        if (!MiruFilter.NO_FILTER.equals(request.query.featureFilter)) {
            constrainFeature = aggregateUtil.filter("strutFeature",
                bitmaps, schema, termComposer,
                context.getFieldIndexProvider(),
                request.query.featureFilter,
                solutionLog,
                null,
                activityIndexLastId,
                -1,
                stackBuffer);
        } else {
            constrainFeature = null;
        }

        BM[] answers = bitmaps.createArrayOf(score.size());
        int[] scoredToLastIds = new int[score.size()];
        Arrays.fill(scoredToLastIds, -1);
        List<Scored> updates = Lists.newArrayList();
        strut.yourStuff("strut",
            handle.getCoord(),
            bitmaps,
            context,
            request,
            (streamBitmaps) -> {

                MiruTermId[] rescoreMiruTermIds = score.toArray(new MiruTermId[0]);
                Arrays.fill(answers, null);
                bitmaps.multiTx(
                    (tx, stackBuffer1) -> primaryIndex.multiTxIndex("strut", pivotFieldId, rescoreMiruTermIds, -1, stackBuffer1, tx),
                    (index, lastId, bitmap) -> {
                        if (constrainFeature != null) {
                            bitmaps.inPlaceAnd(bitmap, constrainFeature);
                        }
                        answers[index] = bitmap;
                        scoredToLastIds[index] = lastId;
                    },
                    stackBuffer);

                for (int i = 0; i < rescoreMiruTermIds.length; i++) {
                    if (answers[i] != null && !streamBitmaps.stream(-1, rescoreMiruTermIds[i], scoredToLastIds[i], answers[i])) {
                        return false;
                    }
                }
                return true;
            },
            new float[]{0.0f},
            (thresholdIndex, hotness, cacheable) -> {
                if (cacheable) {
                    updates.add(hotness);
                }
                return true;
            },
            solutionLog);

        if (!updates.isEmpty()) {
            long startOfUpdates = System.currentTimeMillis();
            modelScorer.commit(request.query.modelId, cacheStores, updates, stackBuffer);
            LOG.info("Strut score updates {} features in {} ms", updates.size(), System.currentTimeMillis() - startOfUpdates);
        }
        return updates;
    }

    private static class LastIdAndTermId {

        private final int lastId;
        private final MiruTermId termId;

        public LastIdAndTermId(int lastId, MiruTermId termId) {
            this.lastId = lastId;
            this.termId = termId;
        }
    }

    @Override
    public MiruPartitionResponse<StrutAnswer> askRemote(MiruHost host,
        MiruPartitionId partitionId,
        Optional<StrutReport> report) throws MiruQueryServiceException {
        return remotePartition.askRemote(host, partitionId, request, report);
    }

    @Override
    public Optional<StrutReport> createReport(Optional<StrutAnswer> answer) {
        return answer.transform(input -> new StrutReport());
    }
}
