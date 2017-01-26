package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
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
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.TimeVersionRealtime;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruPartitionResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.plugin.solution.Question;
import com.jivesoftware.os.miru.stream.plugins.fulltext.FullText;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutModelScorer.LastIdAndTermId;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jonathan
 */
public class StrutQuestion implements Question<StrutQuery, StrutAnswer, StrutReport> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final StrutModelScorer modelScorer;
    private final Strut strut;
    private final FullText fullText;
    private final MiruJustInTimeBackfillerizer backfillerizer;
    private final MiruRequest<StrutQuery> request;
    private final MiruRemotePartition<StrutQuery, StrutAnswer, StrutReport> remotePartition;
    private final int maxTermIdsPerRequest;
    private final boolean allowImmediateRescore;

    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public StrutQuestion(StrutModelScorer modelScorer,
        Strut strut,
        FullText fullText,
        MiruJustInTimeBackfillerizer backfillerizer,
        MiruRequest<StrutQuery> request,
        MiruRemotePartition<StrutQuery, StrutAnswer, StrutReport> remotePartition,
        int maxTermIdsPerRequest,
        boolean allowImmediateRescore) {
        this.modelScorer = modelScorer;
        this.strut = strut;
        this.fullText = fullText;
        this.backfillerizer = backfillerizer;
        this.request = request;
        this.remotePartition = remotePartition;
        this.maxTermIdsPerRequest = maxTermIdsPerRequest;
        this.allowImmediateRescore = allowImmediateRescore;
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
        MiruSchema schema = context.getSchema();
        MiruTermComposer termComposer = context.getTermComposer();

        if (!MiruFilter.NO_FILTER.equals(request.query.constraintFilter)) {
            BM constrained = aggregateUtil.filter("strutGather",
                bitmaps,
                context,
                request.query.constraintFilter,
                solutionLog,
                null,
                activityIndexLastId,
                -1,
                -1,
                stackBuffer);
            ands.add(constrained);
        }

        if (request.query.query != null && !request.query.query.isEmpty()) {
            MiruFilter filter = fullText.parseQuery(request.query.defaultField, request.query.locale, request.query.query);

            BM filtered = aggregateUtil.filter("fullTextCustom",
                bitmaps,
                context,
                filter,
                solutionLog,
                null,
                activityIndexLastId,
                -1,
                request.query.maxWildcardExpansion,
                stackBuffer);
            ands.add(filtered);
        }

        BitmapAndLastId<BM> container = new BitmapAndLastId<>();

        Optional<BM> unreadIndex = Optional.absent();
        if (request.query.unreadStreamId != null
            && !MiruStreamId.NULL.equals(request.query.unreadStreamId)) {
            if (request.query.suppressUnreadFilter != null && handle.canBackfill()) {
                backfillerizer.backfillUnread(bitmaps,
                    context,
                    solutionLog,
                    request.tenantId,
                    handle.getCoord().partitionId,
                    request.query.unreadStreamId,
                    request.query.suppressUnreadFilter);
            }

            context.getUnreadTrackingIndex().getUnread(request.query.unreadStreamId).getIndex(container, stackBuffer);
            unreadIndex = Optional.fromNullable(container.getBitmap());
            if (container.isSet() && request.query.unreadOnly) {
                ands.add(container.getBitmap());
            }
        }

        if (!MiruAuthzExpression.NOT_PROVIDED.equals(request.authzExpression)) {
            ands.add(context.getAuthzIndex().getCompositeAuthz(request.authzExpression, stackBuffer));
        }

        if (!MiruTimeRange.ALL_TIME.equals(request.query.timeRange)) {
            ands.add(bitmaps.buildTimeRangeMask(context.getTimeIndex(), timeRange.smallestTimestamp, timeRange.largestTimestamp, stackBuffer));
        }

        ands.add(bitmaps.buildIndexMask(activityIndexLastId, context.getRemovalIndex(), container, stackBuffer));

        bitmapsDebug.debug(solutionLog, bitmaps, "ands", ands);
        BM eligible = bitmaps.and(ands);

        int pivotFieldId = schema.getFieldId(request.query.constraintField);

        MiruFieldIndex<BM, IBM> primaryIndex = context.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);

        MinMaxPriorityQueue<Scored> scored = MinMaxPriorityQueue
            .expectedSize(request.query.desiredNumberOfResults)
            .maximumSize(request.query.desiredNumberOfResults)
            .create();

        String[] modelIds = new String[request.query.modelScalars.size()];
        MiruPluginCacheProvider.LastIdCacheKeyValues[] termScoresCaches = new MiruPluginCacheProvider.LastIdCacheKeyValues[request.query.modelScalars.size()];
        float[] termScoresCacheScalars = new float[request.query.modelScalars.size()];
        for (int i = 0; i < request.query.modelScalars.size(); i++) {
            StrutModelScalar modelScalar = request.query.modelScalars.get(i);
            modelIds[i] = modelScalar.modelId;
            termScoresCaches[i] = modelScorer.getTermScoreCache(context, modelScalar.catwalkId);
            termScoresCacheScalars[i] = modelScalar.scalar;
        }

        AtomicInteger modelTotalPartitionCount = new AtomicInteger();

        long start = System.currentTimeMillis();
        List<LastIdAndTermId> lastIdAndTermIds = Lists.newArrayList();
        //TODO config batch size
        aggregateUtil.gather("strut", bitmaps, context, eligible, pivotFieldId, 100, true, false, solutionLog, (lastId, termId, count) -> {
            lastIdAndTermIds.add(new LastIdAndTermId(lastId, termId));
            return maxTermIdsPerRequest <= 0 || lastIdAndTermIds.size() < maxTermIdsPerRequest;
        }, stackBuffer);

        solutionLog.log(MiruSolutionLogLevel.INFO, "Strut accumulated {} terms in {} ms", lastIdAndTermIds.size(), System.currentTimeMillis() - start);
        start = System.currentTimeMillis();

        long totalTimeFetchingLastId = 0;
        long totalTimeFetchingScores = 0;
        long totalTimeRescores = 0;

        List<MiruTermId> asyncRescore = Lists.newArrayList();
        float[] maxScore = { 0f };
        if (request.query.usePartitionModelCache) {
            long fetchScoresStart = System.currentTimeMillis();

            int[] scorableToLastIds = new int[lastIdAndTermIds.size()];
            MiruTermId[] nullableMiruTermIds = new MiruTermId[lastIdAndTermIds.size()];
            MiruTermId[] miruTermIds = new MiruTermId[lastIdAndTermIds.size()];

            Arrays.fill(miruTermIds, null);
            for (int i = 0; i < lastIdAndTermIds.size(); i++) {
                miruTermIds[i] = lastIdAndTermIds.get(i).termId;
            }

            System.arraycopy(miruTermIds, 0, nullableMiruTermIds, 0, lastIdAndTermIds.size());
            Arrays.fill(scorableToLastIds, -1);

            long fetchLastIdsStart = System.currentTimeMillis();
            primaryIndex.multiGetLastIds("strut", pivotFieldId, nullableMiruTermIds, scorableToLastIds, stackBuffer);
            totalTimeFetchingLastId += (System.currentTimeMillis() - fetchLastIdsStart);

            StrutModelScorer.score(
                modelIds,
                request.query.numeratorScalars.length,
                miruTermIds,
                termScoresCaches,
                termScoresCacheScalars,
                (termIndex, scores, scoredToLastId) -> {
                    boolean needsRescore = scoredToLastId < scorableToLastIds[termIndex];
                    if (needsRescore) {
                        LOG.inc("strut>scores>rescoreId");
                    }
                    for (int i = 0; i < scores.length; i++) {
                        boolean isNaN = Float.isNaN(scores[i]);
                        if (!needsRescore && isNaN) {
                            LOG.inc("strut>scores>rescoreMissing");
                        }
                        needsRescore |= isNaN;
                        if (!isNaN) {
                            maxScore[0] = Math.max(maxScore[0], scores[i]);
                        }
                    }
                    LastIdAndTermId lastIdAndTermId = lastIdAndTermIds.get(termIndex);
                    if (needsRescore) {
                        asyncRescore.add(lastIdAndTermId.termId);
                    }
                    float scaledScore = Strut.scaleScore(scores, request.query.numeratorScalars, request.query.numeratorStrategy);
                    scored.add(new Scored(lastIdAndTermId.lastId,
                        miruTermIds[termIndex],
                        scoredToLastId,
                        scaledScore,
                        scores,
                        null));
                    return true;
                },
                stackBuffer);
            totalTimeFetchingScores += (System.currentTimeMillis() - fetchScoresStart);
        }

        if (!request.query.usePartitionModelCache || maxScore[0] == 0f && allowImmediateRescore) {
            // TODO FIX for now this takes the first ModelScalar
            StrutModelScalar strutModelScalar = request.query.modelScalars.get(0);

            LOG.info("Performing immediate rescore for coord:{} catwalkId:{} modelId:{} pivotFieldId:{}",
                handle.getCoord(), strutModelScalar.catwalkId, strutModelScalar.modelId, pivotFieldId);
            LOG.inc("strut>rescore>immediate");

            scored.clear();
            asyncRescore.clear();

            MiruPluginCacheProvider.TimestampedCacheKeyValues termFeaturesCache = modelScorer.getTermFeatureCache(context, strutModelScalar.catwalkId);

            BM[] constrainFeature = modelScorer.buildConstrainFeatures(bitmaps,
                context,
                strutModelScalar.catwalkQuery,
                activityIndexLastId,
                stackBuffer,
                solutionLog);

            long rescoreStart = System.currentTimeMillis();
            for (List<LastIdAndTermId> batch : Lists.partition(lastIdAndTermIds, request.query.batchSize)) {
                List<Scored> rescored = modelScorer.rescore(strutModelScalar.catwalkId,
                    strutModelScalar.modelId,
                    strutModelScalar.catwalkQuery,
                    request.query.featureScalars,
                    request.query.featureStrategy,
                    request.query.includeFeatures,
                    request.query.numeratorScalars,
                    request.query.numeratorStrategy,
                    handle,
                    batch,
                    pivotFieldId,
                    constrainFeature,
                    termScoresCaches[0],
                    termFeaturesCache,
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
        TimeVersionRealtime[] timeVersionRealtimes = activityIndex.getAllTimeVersionRealtime("strut", consumeLastIds, stackBuffer);
        long totalTimeAndVersion = System.currentTimeMillis() - timeAndVersionStart;

        for (int j = 0; j < s.length; j++) {
            if (timeVersionRealtimes[j] != null) {
                String[] decomposed = termComposer.decompose(schema, pivotFieldDefinition, stackBuffer, s[j].term);
                hotOrNots.add(new HotOrNot(new MiruValue(decomposed),
                    gatherScoredValues != null ? gatherScoredValues[j] : null,
                    s[j].scaledScore,
                    s[j].features,
                    timeVersionRealtimes[j].timestamp,
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
