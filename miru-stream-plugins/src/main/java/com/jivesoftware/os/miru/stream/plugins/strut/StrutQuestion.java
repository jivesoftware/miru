package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.FilerIO;
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
import com.jivesoftware.os.miru.catwalk.shared.HotOrNot;
import com.jivesoftware.os.miru.catwalk.shared.Scored;
import com.jivesoftware.os.miru.catwalk.shared.StrutModelScalar;
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
import com.jivesoftware.os.miru.plugin.solution.TermIdLastIdCount;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final boolean allowImmediateRescore;
    private final int gatherBatchSize;
    private final boolean gatherParallel;
    private final int scoreConcurrencyLevel;
    private final Set<String> verboseModelIds;
    private final ExecutorService gatherExecutorService;

    private final MiruBitmapsDebug bitmapsDebug = new MiruBitmapsDebug();
    private final MiruAggregateUtil aggregateUtil = new MiruAggregateUtil();

    public StrutQuestion(StrutModelScorer modelScorer,
        Strut strut,
        MiruJustInTimeBackfillerizer backfillerizer,
        MiruRequest<StrutQuery> request,
        MiruRemotePartition<StrutQuery, StrutAnswer, StrutReport> remotePartition,
        int maxTermIdsPerRequest,
        boolean allowImmediateRescore,
        int gatherBatchSize,
        boolean gatherParallel,
        int scoreConcurrencyLevel,
        Set<String> verboseModelIds,
        ExecutorService gatherExecutorService) {
        this.modelScorer = modelScorer;
        this.strut = strut;
        this.backfillerizer = backfillerizer;
        this.request = request;
        this.remotePartition = remotePartition;
        this.maxTermIdsPerRequest = maxTermIdsPerRequest;
        this.allowImmediateRescore = allowImmediateRescore;
        this.gatherBatchSize = gatherBatchSize;
        this.gatherParallel = gatherParallel;
        this.scoreConcurrencyLevel = scoreConcurrencyLevel;
        this.verboseModelIds = verboseModelIds;
        this.gatherExecutorService = gatherExecutorService;
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
            LOG.inc("askLocal>noIntersection");
            return new MiruPartitionResponse<>(answer, solutionLog.asList());
        }

        int activityIndexLastId = context.getActivityIndex().lastId(stackBuffer);

        List<IBM> ands = new ArrayList<>();
        MiruSchema schema = context.getSchema();
        MiruTermComposer termComposer = context.getTermComposer();

        if (!MiruFilter.NO_FILTER.equals(request.query.constraintFilter)) {
            long start = System.currentTimeMillis();
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
            long elapsed = System.currentTimeMillis() - start;
            LOG.inc("askLocal>constrained>pow>" + FilerIO.chunkPower(elapsed, 0));
            solutionLog.log(MiruSolutionLogLevel.INFO, "Constrained in {} ms", elapsed);
        }

        BitmapAndLastId<BM> container = new BitmapAndLastId<>();

        Optional<BM> unreadIndex = Optional.absent();
        if (request.query.unreadStreamId != null
            && !MiruStreamId.NULL.equals(request.query.unreadStreamId)) {
            if (request.query.suppressUnreadFilter != null && handle.canBackfill()) {
                long start = System.currentTimeMillis();
                backfillerizer.backfillUnread(bitmaps,
                    context,
                    solutionLog,
                    request.tenantId,
                    handle.getCoord().partitionId,
                    request.query.unreadStreamId,
                    request.query.suppressUnreadFilter);
                long elapsed = System.currentTimeMillis() - start;
                LOG.inc("askLocal>backfill>pow>" + FilerIO.chunkPower(elapsed, 0));
                solutionLog.log(MiruSolutionLogLevel.INFO, "Backfill unread for {} took {} ms", request.query.unreadStreamId, elapsed);
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
        BM candidates = bitmaps.and(ands);

        int pivotFieldId = schema.getFieldId(request.query.constraintField);

        MiruFieldIndex<BM, IBM> primaryIndex = context.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);

        boolean verboseModelId = false;
        String[] modelIds = new String[request.query.modelScalars.size()];
        MiruPluginCacheProvider.LastIdCacheKeyValues[] termScoresCaches = new MiruPluginCacheProvider.LastIdCacheKeyValues[request.query.modelScalars.size()];
        float[] termScoresCacheScalars = new float[request.query.modelScalars.size()];
        List<IBM> masks = Lists.newArrayList();
        for (int i = 0; i < request.query.modelScalars.size(); i++) {
            StrutModelScalar modelScalar = request.query.modelScalars.get(i);
            modelIds[i] = modelScalar.modelId;
            verboseModelId |= verboseModelIds.contains(modelScalar.modelId);
            termScoresCaches[i] = modelScorer.getTermScoreCache(context, modelScalar.catwalkId);
            termScoresCacheScalars[i] = modelScalar.scalar;
            BM nilBitmap = modelScorer.nilBitmap(context, modelScalar.catwalkId, modelScalar.modelId, stackBuffer);
            if (nilBitmap != null) {
                masks.add(nilBitmap);
            }
            if (verboseModelId) {
                LOG.info("Added nil mask for modelId:{} bits:{}", modelScalar.modelId, nilBitmap == null ? -1 : bitmaps.cardinality(nilBitmap));
            }
        }
        IBM nilMask = masks.isEmpty() ? null : masks.size() == 1 ? masks.get(0) : bitmaps.and(masks);
        BM eligible = nilMask == null ? candidates : bitmaps.andNot(candidates, nilMask);
        if (nilMask != null) {
            LOG.info("Reduced candidates for:{} from:{} nil:{} to:{}", Lists.transform(request.query.modelScalars, scalar -> scalar.modelId),
                bitmaps.cardinality(candidates), bitmaps.cardinality(nilMask), bitmaps.cardinality(eligible));
        }

        AtomicInteger modelTotalPartitionCount = new AtomicInteger();

        long start = System.currentTimeMillis();
        List<TermIdLastIdCount> termIdLastIdCounts = Lists.newArrayList();
        Optional<BM> counter = request.query.countUnread ? unreadIndex.transform(input -> bitmaps.and(Arrays.asList(input, eligible))) : Optional.absent();
        if (gatherParallel) {
            aggregateUtil.gatherParallel("strut",
                bitmaps,
                context,
                eligible,
                pivotFieldId,
                gatherBatchSize,
                true,
                maxTermIdsPerRequest,
                counter,
                solutionLog,
                gatherExecutorService,
                (termIdLastIdCount) -> {
                    termIdLastIdCounts.add(termIdLastIdCount);
                    if (termIdLastIdCount.count <= 0) {
                        LOG.inc("strut>gather>empty");
                    }
                    return maxTermIdsPerRequest <= 0 || termIdLastIdCounts.size() < maxTermIdsPerRequest;
                },
                stackBuffer);
        } else {
            aggregateUtil.gather("strut",
                bitmaps,
                context,
                eligible,
                pivotFieldId,
                gatherBatchSize,
                false,
                true,
                counter,
                solutionLog,
                (lastId, termId, count) -> {
                    termIdLastIdCounts.add(new TermIdLastIdCount(termId, lastId, count));
                    if (count <= 0) {
                        LOG.inc("strut>gather>empty");
                    }
                    return maxTermIdsPerRequest <= 0 || termIdLastIdCounts.size() < maxTermIdsPerRequest;
                },
                stackBuffer);
        }

        if (verboseModelId) {
            LOG.info("Gathered modelIds:{} count:{}", Arrays.toString(modelIds), termIdLastIdCounts.size());
        }

        long elapsed = System.currentTimeMillis() - start;
        LOG.inc("askLocal>accumulated>pow>" + FilerIO.chunkPower(elapsed, 0));
        solutionLog.log(MiruSolutionLogLevel.INFO, "Strut accumulated {} terms in {} ms", termIdLastIdCounts.size(), elapsed);
        start = System.currentTimeMillis();

        long totalTimeFetchingLastId = 0;
        long totalTimeFetchingScores = 0;
        long totalTimeRescores = 0;

        float[] maxScore = { 0f };
        long fetchScoresStart = System.currentTimeMillis();

        int[] scorableToLastIds = new int[termIdLastIdCounts.size()];
        MiruTermId[] nullableMiruTermIds = new MiruTermId[termIdLastIdCounts.size()];
        MiruTermId[] miruTermIds = new MiruTermId[termIdLastIdCounts.size()];

        Arrays.fill(miruTermIds, null);
        for (int i = 0; i < termIdLastIdCounts.size(); i++) {
            miruTermIds[i] = termIdLastIdCounts.get(i).termId;
        }

        System.arraycopy(miruTermIds, 0, nullableMiruTermIds, 0, termIdLastIdCounts.size());
        Arrays.fill(scorableToLastIds, -1);

        long fetchLastIdsStart = System.currentTimeMillis();
        primaryIndex.multiGetLastIds("strut", pivotFieldId, nullableMiruTermIds, scorableToLastIds, stackBuffer);
        totalTimeFetchingLastId += (System.currentTimeMillis() - fetchLastIdsStart);

        @SuppressWarnings("unchecked")
        MinMaxPriorityQueue<Scored>[] parallelScored = new MinMaxPriorityQueue[scoreConcurrencyLevel];
        for (int i = 0; i < parallelScored.length; i++) {
            parallelScored[i] = MinMaxPriorityQueue
                .expectedSize(request.query.desiredNumberOfResults)
                .maximumSize(request.query.desiredNumberOfResults)
                .create();
        }

        boolean scoreVerboseModelId = verboseModelId;
        StrutModelScorer.scoreParallel(
            modelIds,
            request.query.numeratorScalars.length,
            miruTermIds,
            scoreConcurrencyLevel,
            termScoresCaches,
            termScoresCacheScalars,
            (bucket, termIndex, scores, scoredToLastId) -> {
                TermIdLastIdCount termIdLastIdCount = termIdLastIdCounts.get(termIndex);
                if (scoredToLastId != -1) {
                    for (int i = 0; i < scores.length; i++) {
                        if (Float.isNaN(scores[i])) {
                            LOG.inc("strut>scores>NaN");
                        } else {
                            maxScore[0] = Math.max(maxScore[0], scores[i]);
                        }
                    }
                    float scaledScore = Strut.scaleScore(scores, request.query.numeratorScalars, request.query.numeratorStrategy);
                    parallelScored[bucket].add(new Scored(termIdLastIdCount.lastId,
                        miruTermIds[termIndex],
                        scoredToLastId,
                        scaledScore,
                        scores,
                        null,
                        termIdLastIdCount.count));
                }
                if (scoreVerboseModelId) {
                    LOG.info("Retrieved score for modelIds:{} termId:{} scores:{} lastId:{} count:{} scoredToLastId:{}",
                        Arrays.toString(modelIds),
                        termIdLastIdCount.termId,
                        Arrays.toString(scores),
                        termIdLastIdCount.lastId,
                        termIdLastIdCount.count,
                        scoredToLastId);
                }
                return true;
            },
            gatherExecutorService,
            stackBuffer);

        MinMaxPriorityQueue<Scored> scored = MinMaxPriorityQueue
            .expectedSize(request.query.desiredNumberOfResults)
            .maximumSize(request.query.desiredNumberOfResults)
            .create();

        for (int i = 0; i < scoreConcurrencyLevel; i++) {
            scored.addAll(parallelScored[i]);
        }

        totalTimeFetchingScores += (System.currentTimeMillis() - fetchScoresStart);

        modelScorer.enqueue(handle.getCoord(), request.query, pivotFieldId);

        MiruFieldDefinition[] gatherFieldDefinitions = null;
        if (request.query.gatherTermsForFields != null && request.query.gatherTermsForFields.length > 0) {
            gatherFieldDefinitions = new MiruFieldDefinition[request.query.gatherTermsForFields.length];
            for (int i = 0; i < gatherFieldDefinitions.length; i++) {
                gatherFieldDefinitions[i] = schema.getFieldDefinition(schema.getFieldId(request.query.gatherTermsForFields[i]));
                Preconditions.checkArgument(gatherFieldDefinitions[i].type.hasFeature(MiruFieldDefinition.Feature.stored),
                    "You can only gather stored fields");
            }
        }

        MiruActivityIndex activityIndex = context.getActivityIndex();
        MiruFieldDefinition pivotFieldDefinition = schema.getFieldDefinition(pivotFieldId);
        List<HotOrNot> hotOrNots = new ArrayList<>(request.query.desiredNumberOfResults);

        Scored[] s = scored.toArray(new Scored[0]);
        if (scored.size() < request.query.desiredNumberOfResults) {
            int remaining = request.query.desiredNumberOfResults - scored.size();
            Set<MiruTermId> gathered = Sets.newHashSet();
            for (Scored scored1 : scored) {
                gathered.add(scored1.term);
            }

            List<TermIdLastIdCount> nils = Lists.newArrayList();
            BM nilCandidates = nilMask == null ? candidates : bitmaps.and(Arrays.asList(candidates, nilMask));
            aggregateUtil.gather("strut",
                bitmaps,
                context,
                nilCandidates,
                pivotFieldId,
                gatherBatchSize,
                true,
                true,
                counter,
                solutionLog,
                (lastId, termId, count) -> {
                    if (gathered.add(termId)) {
                        nils.add(new TermIdLastIdCount(termId, lastId, count));
                    }
                    if (count <= 0) {
                        LOG.inc("strut>nilGather>empty");
                    }
                    return nils.size() < remaining;
                },
                stackBuffer);

            Scored[] scoredArray = s;
            s = new Scored[scoredArray.length + nils.size()];
            System.arraycopy(scoredArray, 0, s, 0, scoredArray.length);

            for (int i = 0; i < nils.size(); i++) {
                TermIdLastIdCount termIdLastIdCount = nils.get(i);
                s[i + scoredArray.length] = new Scored(termIdLastIdCount.lastId,
                    termIdLastIdCount.termId,
                    -1,
                    0f,
                    new float[request.query.numeratorScalars.length],
                    null,
                    termIdLastIdCount.count);
            }
            LOG.inc("strut>nilGather>size>pow>" + FilerIO.chunkPower(nils.size(), 0));
            LOG.inc("strut>nilGather>count", nils.size());
        } else {
            LOG.inc("strut>nilGather>skip");
        }

        int[] scoredLastIds = new int[s.length];
        for (int j = 0; j < s.length; j++) {
            scoredLastIds[j] = s[j].lastId;
        }

        long gatherStart = System.currentTimeMillis();
        MiruValue[][][] gatherScoredValues = null;
        if (gatherFieldDefinitions != null) {
            gatherScoredValues = new MiruValue[scoredLastIds.length][gatherFieldDefinitions.length][];
            int[] consumeLastIds = new int[scoredLastIds.length];
            for (int j = 0; j < gatherFieldDefinitions.length; j++) {
                System.arraycopy(scoredLastIds, 0, consumeLastIds, 0, scoredLastIds.length);
                MiruTermId[][] termIds = activityIndex.getAll("strut", consumeLastIds, gatherFieldDefinitions[j], stackBuffer);
                for (int k = 0; k < termIds.length; k++) {
                    if (termIds[k] != null) {
                        gatherScoredValues[k][j] = new MiruValue[termIds[k].length];
                        for (int l = 0; l < termIds[k].length; l++) {
                            gatherScoredValues[k][j][l] = new MiruValue(termComposer.decompose(schema,
                                gatherFieldDefinitions[j],
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
                    unreadIndex.isPresent() && bitmaps.isSet(unreadIndex.get(), s[j].lastId),
                    s[j].count));
            } else {
                LOG.warn("Failed to get timestamp for {}", scoredLastIds[j]);
            }
        }

        elapsed = System.currentTimeMillis() - start;
        LOG.inc("askLocal>strutYourStuff>pow>" + FilerIO.chunkPower(elapsed, 0));
        solutionLog.log(MiruSolutionLogLevel.INFO,
            "Strut your stuff for {} terms took"
                + " lastIds {} ms,"
                + " cached {} ms,"
                + " rescore {} ms,"
                + " gather {} ms,"
                + " timeAndVersion {} ms,"
                + " total {} ms",
            termIdLastIdCounts.size(),
            totalTimeFetchingLastId,
            totalTimeFetchingScores,
            totalTimeRescores,
            totalTimeGather,
            totalTimeAndVersion,
            elapsed);

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
