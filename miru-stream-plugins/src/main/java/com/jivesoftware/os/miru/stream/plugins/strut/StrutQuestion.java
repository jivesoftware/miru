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
import com.jivesoftware.os.miru.plugin.backfill.MiruJustInTimeBackfillerizer;
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
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkQuery;
import com.jivesoftware.os.miru.stream.plugins.strut.Strut.Scored;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutModelScorer.LastIdAndTermId;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan
 */
public class StrutQuestion implements Question<StrutQuery, StrutAnswer, StrutReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final StrutModelScorer modelScorer;
    private final Strut strut;
    private final MiruJustInTimeBackfillerizer backfillerizer;
    private final MiruRequest<StrutQuery> request;
    private final MiruRemotePartition<StrutQuery, StrutAnswer, StrutReport> remotePartition;
    private final int maxTermIdsPerRequest;
    private final int maxUpdatesBeforeFlush;

    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public StrutQuestion(StrutModelScorer modelScorer,
        Strut strut,
        MiruJustInTimeBackfillerizer backfillerizer,
        MiruRequest<StrutQuery> request,
        MiruRemotePartition<StrutQuery, StrutAnswer, StrutReport> remotePartition,
        int maxTermIdsPerRequest,
        int maxUpdatesBeforeFlush) {
        this.modelScorer = modelScorer;
        this.strut = strut;
        this.backfillerizer = backfillerizer;
        this.request = request;
        this.remotePartition = remotePartition;
        this.maxTermIdsPerRequest = maxTermIdsPerRequest;
        this.maxUpdatesBeforeFlush = maxUpdatesBeforeFlush;
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
            StrutAnswer answer = strut.composeAnswer(context, request, Collections.emptyList(), 0);
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

        Optional<BM> unreadIndex = Optional.absent();
        if (request.query.unreadStreamId != null) {
            if (handle.canBackfill()) {
                backfillerizer.backfillUnread(bitmaps,
                    context,
                    solutionLog,
                    request.tenantId,
                    handle.getCoord().partitionId,
                    request.query.unreadStreamId);
            }

            unreadIndex = context.getUnreadTrackingIndex().getUnread(request.query.unreadStreamId).getIndex(stackBuffer);
            if (unreadIndex.isPresent() && request.query.unreadOnly) {
                ands.add(unreadIndex.get());
            }
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

        CatwalkQuery catwalkQuery = request.query.catwalkQuery;
        MinMaxPriorityQueue<Scored> scored = MinMaxPriorityQueue
            .expectedSize(request.query.desiredNumberOfResults)
            .maximumSize(request.query.desiredNumberOfResults)
            .create();

        int payloadSize = 4 * request.query.catwalkQuery.gatherFilters.length + 4; // this is amazing
        MiruPluginCacheProvider.CacheKeyValues cacheStores = handle.getRequestContext()
            .getCacheProvider()
            .get("strut-" + request.query.catwalkId, payloadSize, false, maxUpdatesBeforeFlush);

        AtomicInteger modelTotalPartitionCount = new AtomicInteger();

        long start = System.currentTimeMillis();
        List<LastIdAndTermId> lastIdAndTermIds = Lists.newArrayList();
        //TODO config batch size
        aggregateUtil.gather("strut", bitmaps, context, eligible, pivotFieldId, 100, true, solutionLog, (lastId, termId) -> {
            lastIdAndTermIds.add(new LastIdAndTermId(lastId, termId));
            return maxTermIdsPerRequest <= 0 || lastIdAndTermIds.size() < maxTermIdsPerRequest;
        }, stackBuffer);

        solutionLog.log(MiruSolutionLogLevel.INFO, "Strut accumulated {} terms in {} ms", lastIdAndTermIds.size(), System.currentTimeMillis() - start);
        start = System.currentTimeMillis();

        long totalTimeFetchingLastId = 0;
        long totalTimeFetchingScores = 0;
        long totalTimeRescores = 0;

        List<LastIdAndTermId> asyncRescore = Lists.newArrayList();
        boolean[] needsImmediateRescore = { true };
        if (request.query.usePartitionModelCache) {
            long fetchScoresStart = System.currentTimeMillis();

            int[] scoredToLastIds = new int[lastIdAndTermIds.size()];
            MiruTermId[] nullableMiruTermIds = new MiruTermId[lastIdAndTermIds.size()];
            MiruTermId[] miruTermIds = new MiruTermId[lastIdAndTermIds.size()];

            Arrays.fill(miruTermIds, null);
            for (int i = 0; i < lastIdAndTermIds.size(); i++) {
                miruTermIds[i] = lastIdAndTermIds.get(i).termId;
            }

            System.arraycopy(miruTermIds, 0, nullableMiruTermIds, 0, lastIdAndTermIds.size());
            Arrays.fill(scoredToLastIds, -1);

            long fetchLastIdsStart = System.currentTimeMillis();
            primaryIndex.multiGetLastIds("strut", pivotFieldId, nullableMiruTermIds, scoredToLastIds, stackBuffer);
            totalTimeFetchingLastId += (System.currentTimeMillis() - fetchLastIdsStart);

            StrutModelScorer.score(
                request.query.modelId,
                catwalkQuery.gatherFilters.length,
                miruTermIds,
                cacheStores,
                (termIndex, scores, scoredToLastId) -> {
                    boolean needsRescore = scoredToLastId < scoredToLastIds[termIndex];
                    if (needsRescore) {
                        LOG.inc("strut>scores>rescoreId");
                    }
                    for (int i = 0; !needsRescore && i < scores.length; i++) {
                        needsRescore = Float.isNaN(scores[i]);
                        if (needsRescore) {
                            LOG.inc("strut>scores>rescoreNaN");
                        }
                    }
                    if (needsRescore) {
                        asyncRescore.add(lastIdAndTermIds.get(termIndex));
                    } else {
                        needsImmediateRescore[0] = false;
                    }
                    float scaledScore = Strut.scaleScore(scores, request.query.numeratorScalars, request.query.numeratorStrategy);
                    scored.add(new Scored(lastIdAndTermIds.get(termIndex).lastId,
                        miruTermIds[termIndex],
                        scoredToLastIds[termIndex],
                        scaledScore,
                        scores,
                        null));
                    return true;
                },
                stackBuffer);
            totalTimeFetchingScores += (System.currentTimeMillis() - fetchScoresStart);
        }

        if (needsImmediateRescore[0]) {
            LOG.info("Performing immediate rescore for coord:{} catwalkId:{} modelId:{} pivotFieldId:{}",
                handle.getCoord(), request.query.catwalkId, request.query.modelId, pivotFieldId);
            LOG.inc("strut>rescore>immediate");
            BM[] answers = bitmaps.createArrayOf(request.query.batchSize);
            BM[] constrainFeature = modelScorer.buildConstrainFeatures(bitmaps,
                context,
                request.query.featureFilter,
                catwalkQuery,
                activityIndexLastId,
                stackBuffer,
                solutionLog);
            long rescoreStart = System.currentTimeMillis();
            for (List<LastIdAndTermId> batch : Lists.partition(lastIdAndTermIds, answers.length)) {
                List<Scored> rescored = modelScorer.rescore(request.query.catwalkId,
                    request.query.modelId,
                    request.query.catwalkQuery,
                    request.query.featureScalars,
                    request.query.featureStrategy,
                    request.query.includeFeatures,
                    request.query.numeratorScalars,
                    request.query.numeratorStrategy,
                    handle,
                    batch,
                    pivotFieldId,
                    constrainFeature,
                    cacheStores,
                    modelTotalPartitionCount,
                    solutionLog);
                scored.addAll(rescored);
            }
            totalTimeRescores += (System.currentTimeMillis() - rescoreStart);

        } else if (!asyncRescore.isEmpty()) {
            LOG.inc("strut>rescore>async");
            modelScorer.enqueue(handle.getCoord(), request.query, pivotFieldId, asyncRescore);
        } else {
            LOG.inc("strut>rescore>none");
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

        long gatherStart = System.currentTimeMillis();
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
        long totalTimeGather = System.currentTimeMillis() - gatherStart;

        long timeAndVersionStart = System.currentTimeMillis();
        int[] consumeLastIds = new int[scoredLastIds.length];
        System.arraycopy(scoredLastIds, 0, consumeLastIds, 0, scoredLastIds.length);
        TimeAndVersion[] timeAndVersions = activityIndex.getAllTimeAndVersions("strut", consumeLastIds, stackBuffer);
        long totalTimeAndVersion = System.currentTimeMillis() - timeAndVersionStart;

        for (int j = 0; j < s.length; j++) {
            if (timeAndVersions[j] != null) {
                String[] decomposed = termComposer.decompose(schema, pivotFieldDefinition, stackBuffer, s[j].term);
                hotOrNots.add(new HotOrNot(new MiruValue(decomposed),
                    gatherScoredValues != null ? gatherScoredValues[j] : null,
                    s[j].scaledScore,
                    s[j].features,
                    timeAndVersions[j].timestamp,
                    unreadIndex.isPresent() && bitmaps.isSet(unreadIndex.get(), s[j].lastId)));
            } else {
                LOG.warn("Failed to get timestamp for {}", scoredLastIds[j]);
            }
        }

        solutionLog.log(MiruSolutionLogLevel.INFO,
            "Strut your stuff for {} terms took"
                + " lastIds {} ms,"
                + " cached {} ms,"
                + " rescore {} ms,"
                + " gather {} ms,"
                + " timeAndVersion {} ms,"
                + " total {} ms",
            lastIdAndTermIds.size(),
            totalTimeFetchingLastId,
            totalTimeFetchingScores,
            totalTimeRescores,
            totalTimeGather,
            totalTimeAndVersion,
            System.currentTimeMillis() - start);

        solutionLog.log(MiruSolutionLogLevel.INFO, "Strut found {} terms", hotOrNots.size());

        return new MiruPartitionResponse<>(strut.composeAnswer(context, request, hotOrNots, modelTotalPartitionCount.get()), solutionLog.asList());
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
