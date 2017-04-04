package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery;
import com.jivesoftware.os.miru.catwalk.shared.Scored;
import com.jivesoftware.os.miru.catwalk.shared.StrutModelScalar;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.CacheKeyBitmaps;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.LastIdCacheKeyValues;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.TimestampedCacheKeyValues;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.FieldMultiTermTxIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.miru.plugin.partition.OrderedPartitions;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.plugin.solution.TermIdLastIdCount;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutQuery.Strategy;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class StrutModelScorer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public interface ScoredStream {

        boolean score(int termIndex, float[] scores, int lastId);
    }

    public interface ParallelScoredStream {

        boolean score(int bucket, int termIndex, float[] scores, int lastId);
    }

    private final MiruProvider<? extends Miru> miruProvider;
    private final Strut strut;
    private final StrutRemotePartition strutRemotePartition;
    private final MiruAggregateUtil aggregateUtil;
    private final AtomicLong pendingUpdates;
    private final int topNValuesPerFeature;
    private final long maxHeapPressureInBytes;
    private final double hashIndexLoadFactor;
    private final boolean shareScores;
    private final float nilScoreThreshold;

    private final LinkedHashMap<StrutQueueKey, Enqueued>[] queues;

    private final AtomicBoolean running = new AtomicBoolean();
    private final List<Future<?>> futures = Lists.newArrayList();

    public StrutModelScorer(MiruProvider<? extends Miru> miruProvider,
        Strut strut,
        StrutRemotePartition strutRemotePartition,
        MiruAggregateUtil aggregateUtil,
        AtomicLong pendingUpdates,
        int topNValuesPerFeature,
        long maxHeapPressureInBytes,
        double hashIndexLoadFactor,
        int queueStripeCount,
        boolean shareScores,
        float nilScoreThreshold) {
        this.miruProvider = miruProvider;
        this.strut = strut;
        this.strutRemotePartition = strutRemotePartition;
        this.aggregateUtil = aggregateUtil;
        this.pendingUpdates = pendingUpdates;
        this.topNValuesPerFeature = topNValuesPerFeature;
        this.maxHeapPressureInBytes = maxHeapPressureInBytes;
        this.hashIndexLoadFactor = hashIndexLoadFactor;
        this.shareScores = shareScores;
        this.nilScoreThreshold = nilScoreThreshold;

        this.queues = new LinkedHashMap[queueStripeCount];
        for (int i = 0; i < queueStripeCount; i++) {
            queues[i] = Maps.newLinkedHashMap();
        }
    }

    public void start(ScheduledExecutorService executorService, int queueStripeCount, long consumeIntervalMillis) {
        running.set(true);
        for (int i = 0; i < queueStripeCount; i++) {
            LinkedHashMap<StrutQueueKey, Enqueued> queue = queues[i];
            futures.add(executorService.scheduleWithFixedDelay(() -> {
                try {
                    consume(queue);
                } catch (Throwable t) {
                    LOG.error("Failure while consuming strut model queue", t);
                }
            }, consumeIntervalMillis, consumeIntervalMillis, TimeUnit.MILLISECONDS));
        }
    }

    public void stop() {
        running.set(false);
        for (Future<?> future : futures) {
            future.cancel(true);
        }
    }

    <BM extends IBM, IBM> BM nilBitmap(MiruRequestContext<BM, IBM, ?> context, String catwalkId, String modelId, StackBuffer stackBuffer) throws Exception {
        CacheKeyBitmaps<BM, IBM> nilTermCache = getNilTermCache(context, catwalkId);
        return nilTermCache.get(modelId.getBytes(StandardCharsets.UTF_8), stackBuffer);
    }

    static void scoreParallel(String[] modelId,
        int numeratorsCount,
        MiruTermId[] termIds,
        int concurrencyLevel,
        final LastIdCacheKeyValues[] termScoreCaches,
        float[] termScoreCacheScalars,
        ParallelScoredStream parallelScoredStream,
        ExecutorService executorService,
        StackBuffer stackBuffer) throws Exception {

        int batchSize = (termIds.length + concurrencyLevel - 1) / concurrencyLevel;
        List<Future<?>> futures = Lists.newArrayList();
        for (int i = 0, j = 0; j < termIds.length; i++, j += batchSize) {
            int bucket = i;
            int offset = j;
            int length = Math.min(batchSize, termIds.length - offset);
            futures.add(executorService.submit(() -> {
                scoreInternal(modelId, numeratorsCount, termIds, offset, length, termScoreCaches, termScoreCacheScalars,
                    (termIndex, scores, lastId) -> {
                        return parallelScoredStream.score(bucket, termIndex, scores, lastId);
                    },
                    stackBuffer);
                return null;
            }));
        }
        for (Future<?> future : futures) {
            future.get();
        }
    }

    static void score(String[] modelId,
        int numeratorsCount,
        MiruTermId[] termIds,
        final LastIdCacheKeyValues[] termScoreCaches,
        float[] termScoreCacheScalars,
        ScoredStream scoredStream,
        StackBuffer stackBuffer) throws Exception {

        scoreInternal(modelId, numeratorsCount, termIds, 0, termIds.length, termScoreCaches, termScoreCacheScalars, scoredStream, stackBuffer);
    }

    static void scoreInternal(String[] modelId,
        int numeratorsCount,
        MiruTermId[] termIds,
        int offset,
        int length,
        LastIdCacheKeyValues[] termScoreCaches,
        float[] termScoreCacheScalars,
        ScoredStream scoredStream,
        StackBuffer stackBuffer) throws Exception {

        byte[][] keys = new byte[length][];
        for (int i = 0; i < length; i++) {
            MiruTermId termId = termIds[offset + i];
            if (termId != null) {
                keys[i] = termId.getBytes();
            }
        }

        float[][] scores = new float[length][numeratorsCount];
        int[] lastIds = new int[length];

        float sumOfScalars = 0;
        for (int c = 0; c < termScoreCacheScalars.length; c++) {
            LastIdCacheKeyValues termScoreCache = termScoreCaches[c];
            float termScoreCacheScalar = termScoreCacheScalars[c];
            sumOfScalars += termScoreCacheScalar;
            termScoreCache.get(modelId[c].getBytes(StandardCharsets.UTF_8), keys, (index, value, lastId) -> {
                if (value != null && value.capacity() == (4 * numeratorsCount)) {
                    int valueOffset = 0;
                    for (int n = 0; n < numeratorsCount; n++) {
                        scores[index][n] += (value.getFloat(valueOffset) * termScoreCacheScalar);
                        valueOffset += 4;
                    }
                } else {
                    if (value != null) {
                        LOG.warn("Ignored strut model score for cache:{} model:{} with invalid length {}", termScoreCache.name(), modelId, value.capacity());
                    }
                    Arrays.fill(scores[index], Float.NaN);
                    lastId = -1;
                }
                lastIds[index] = lastId;
                return true;
            }, stackBuffer);
        }

        for (int i = 0; i < length; i++) {
            for (int n = 0; n < numeratorsCount; n++) {
                scores[i][n] /= sumOfScalars;
            }
            if (!scoredStream.score(offset + i, scores[i], lastIds[i])) {
                return;
            }
        }
    }

    private <BM extends IBM, IBM> void commitAndNil(String modelId,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> requestContext,
        int pivotFieldId,
        LastIdCacheKeyValues termScoreCache,
        CacheKeyBitmaps<BM, IBM> nilTermCache,
        List<Scored> updates,
        StackBuffer stackBuffer) throws Exception {

        CommitResult commitResult = commit(modelId, nilScoreThreshold, termScoreCache, updates, stackBuffer);
        List<MiruTermId> hadTermIds = commitResult.hadTermIds;
        List<MiruTermId> nilTermIds = commitResult.nilTermIds;

        byte[] modelIdBytes = modelId.getBytes(StandardCharsets.UTF_8);
        MiruFieldIndex<BM, IBM> primaryFieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        FieldMultiTermTxIndex<BM, IBM> multiTermTxIndex = new FieldMultiTermTxIndex<>("strutScoreCommit", primaryFieldIndex, pivotFieldId, -1);
        if (!hadTermIds.isEmpty()) {
            multiTermTxIndex.setTermIds(hadTermIds.toArray(new MiruTermId[0]));
            BM had = bitmaps.orMultiTx(multiTermTxIndex, stackBuffer);
            nilTermCache.andNot(modelIdBytes, had, stackBuffer);
        }
        if (!nilTermIds.isEmpty()) {
            multiTermTxIndex.setTermIds(nilTermIds.toArray(new MiruTermId[0]));
            BM nil = bitmaps.orMultiTx(multiTermTxIndex, stackBuffer);
            nilTermCache.or(modelIdBytes, nil, stackBuffer);
        }
    }

    static CommitResult commit(String modelId,
        float nilScoreThreshold,
        LastIdCacheKeyValues termScoreCache,
        List<Scored> updates,
        StackBuffer stackBuffer) throws Exception {

        List<MiruTermId> hadTermIds = Lists.newArrayList();
        List<MiruTermId> nilTermIds = Lists.newArrayList();
        byte[] modelIdBytes = modelId.getBytes(StandardCharsets.UTF_8);
        termScoreCache.put(modelIdBytes,
            false,
            false,
            stream -> {
                for (Scored update : updates) {
                    float maxScore = 0f;
                    for (int j = 0; j < update.scores.length; j++) {
                        maxScore = Math.max(maxScore, update.scores[j]);
                    }

                    if (maxScore > nilScoreThreshold) {
                        hadTermIds.add(update.term);
                        byte[] payload = new byte[4 * update.scores.length];
                        int offset = 0;
                        for (int j = 0; j < update.scores.length; j++) {
                            float score = update.scores[j];
                            if (Float.isNaN(score)) {
                                LOG.warn("Encountered NaN score for cache:{} model:{} term:{}", termScoreCache.name(), modelId, update.term);
                                score = 0f;
                            }
                            byte[] scoreBytes = FilerIO.floatBytes(score);
                            System.arraycopy(scoreBytes, 0, payload, offset, 4);
                            offset += 4;
                        }

                        byte[] key = update.term.getBytes();
                        if (!stream.stream(key, payload, update.scoredToLastId)) {
                            return false;
                        }
                    } else {
                        nilTermIds.add(update.term);
                        return true;
                    }
                }
                return true;
            },
            stackBuffer);

        return new CommitResult(hadTermIds, nilTermIds);
    }

    public static class CommitResult {
        public final List<MiruTermId> hadTermIds;
        public final List<MiruTermId> nilTermIds;

        public CommitResult(List<MiruTermId> hadTermIds, List<MiruTermId> nilTermIds) {
            this.hadTermIds = hadTermIds;
            this.nilTermIds = nilTermIds;
        }
    }

    private void shareOut(MiruPartitionCoord coord, StrutShare share) throws Exception {
        OrderedPartitions<?, ?> orderedPartitions = miruProvider.getMiru(coord.tenantId).getOrderedPartitions("strut/share", "strutShare", coord);
        strutRemotePartition.shareRemote("strutShare", coord, orderedPartitions, share);
    }

    void shareIn(MiruPartitionCoord coord, StrutShare share) throws Exception {
        Optional<? extends MiruQueryablePartition<?, ?>> optionalQueryablePartition = miruProvider.getMiru(coord.tenantId).getQueryablePartition(coord);
        if (optionalQueryablePartition.isPresent()) {
            MiruQueryablePartition<?, ?> replica = optionalQueryablePartition.get();
            long start = System.currentTimeMillis();
            shareCommit((MiruQueryablePartition) replica, coord, share);
            long elapsed = System.currentTimeMillis() - start;
            LOG.info("Strut recorded shared updates for {} features in {} ms for {}", share.updates.size(), elapsed, coord);
        }
    }

    private <BM extends IBM, IBM> void shareCommit(MiruQueryablePartition<BM, IBM> replica, MiruPartitionCoord coord, StrutShare share) {
        try (MiruRequestHandle<BM, IBM, ?> handle = replica.acquireQueryHandle()) {
            MiruRequestContext<BM, IBM, ? extends MiruSipCursor<?>> context = handle.getRequestContext();
            MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();
            LastIdCacheKeyValues termScoreCache = getTermScoreCache(context, share.catwalkQuery.catwalkId);
            CacheKeyBitmaps<BM, IBM> nilTermCache = getNilTermCache(context, share.catwalkQuery.catwalkId);
            commitAndNil(share.modelId, bitmaps, context, share.pivotFieldId, termScoreCache, nilTermCache, share.updates, new StackBuffer());
        } catch (Exception e) {
            LOG.warn("Failed to commit shared strut updates for {}", new Object[] { coord }, e);
        }
    }

    void enqueue(MiruPartitionCoord coord, StrutQuery strutQuery, int pivotFieldId) {

        for (StrutModelScalar modelScalar : strutQuery.modelScalars) {

            CatwalkDefinition catwalkDefinition = new CatwalkDefinition(modelScalar.catwalkId,
                modelScalar.catwalkQuery,
                strutQuery.numeratorScalars,
                strutQuery.numeratorStrategy,
                strutQuery.featureScalars,
                strutQuery.featureStrategy);

            StrutQueueKey key = new StrutQueueKey(coord, modelScalar.catwalkId, modelScalar.modelId, pivotFieldId);
            int stripe = Math.abs(key.hashCode() % queues.length);
            synchronized (queues[stripe]) {
                queues[stripe].computeIfAbsent(key, (key1) -> new Enqueued(catwalkDefinition));
            }
            pendingUpdates.incrementAndGet();
        }
    }

    private static class Enqueued {

        final CatwalkDefinition catwalkDefinition;

        public Enqueued(CatwalkDefinition catwalkDefinition) {
            this.catwalkDefinition = catwalkDefinition;
        }

    }

    private void consume(LinkedHashMap<StrutQueueKey, Enqueued> queue) throws Exception {
        LOG.inc("strut>scorer>runs");
        StackBuffer stackBuffer = new StackBuffer();
        MiruSolutionLog solutionLog = new MiruSolutionLog(MiruSolutionLogLevel.NONE);
        while (!queue.isEmpty() && running.get()) {
            Entry<StrutQueueKey, Enqueued> entry = null;
            synchronized (queue) {
                Iterator<Entry<StrutQueueKey, Enqueued>> iter = queue.entrySet().iterator();
                if (iter.hasNext()) {
                    entry = iter.next();
                    iter.remove();
                }
            }

            if (entry != null) {
                LOG.inc("strut>scorer>consumed");
                StrutModelScorer.StrutQueueKey key = entry.getKey();
                Enqueued enqueued = entry.getValue();
                try {
                    Optional<? extends MiruQueryablePartition<?, ?>> optionalQueryablePartition = miruProvider.getMiru(key.coord.tenantId)
                        .getQueryablePartition(key.coord);
                    if (optionalQueryablePartition.isPresent()) {
                        MiruQueryablePartition<?, ?> replica = optionalQueryablePartition.get();

                        try {
                            process((MiruQueryablePartition) replica, key.catwalkId, key.modelId, key.pivotFieldId, enqueued.catwalkDefinition,
                                stackBuffer, solutionLog);
                            LOG.inc("strut>scorer>processed");
                        } catch (Exception e) {
                            LOG.inc("strut>scorer>failed");
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Failed to consume catwalkId:{} modelId:{} pivotFieldId:{}",
                                    new Object[] { key.catwalkId, key.modelId, key.pivotFieldId }, e);
                            } else {
                                LOG.warn("Failed to consume catwalkId:{} modelId:{} pivotFieldId:{} message:{}",
                                    key.catwalkId, key.modelId, key.pivotFieldId, e.getMessage());
                            }
                        }
                    } else {
                        LOG.inc("strut>scorer>ignored");
                    }
                } finally {
                    pendingUpdates.decrementAndGet();
                }
            }
        }
    }

    private <BM extends IBM, IBM> void process(MiruQueryablePartition<BM, IBM> replica,
        String catwalkId,
        String modelId,
        int pivotFieldId,
        CatwalkDefinition catwalkDefinition,
        StackBuffer stackBuffer,
        MiruSolutionLog solutionLog) throws Exception {

        try (MiruRequestHandle<BM, IBM, ?> handle = replica.acquireQueryHandle()) {
            MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();
            MiruRequestContext<BM, IBM, ? extends MiruSipCursor<?>> context = handle.getRequestContext();
            CacheKeyBitmaps<BM, IBM> nilTermCache = getNilTermCache(context, catwalkDefinition.catwalkQuery.catwalkId);

            byte[] modelIdBytes = modelId.getBytes(StandardCharsets.UTF_8);
            int cursorId = nilTermCache.getLastId(modelIdBytes);
            int activityIndexLastId = context.getActivityIndex().lastId(stackBuffer);

            if (activityIndexLastId > cursorId) {
                TimestampedCacheKeyValues termFeatureCache = getTermFeatureCache(context, catwalkId);
                LastIdCacheKeyValues termScoreCache = getTermScoreCache(context, catwalkDefinition.catwalkQuery.catwalkId);
                BM[] asyncConstrainFeature = buildConstrainFeatures(bitmaps,
                    context,
                    catwalkDefinition.catwalkQuery,
                    activityIndexLastId,
                    stackBuffer,
                    solutionLog);
                BM answer = bitmaps.createWithRange(cursorId + 1, activityIndexLastId + 1);
                List<TermIdLastIdCount> rescorable = Lists.newArrayList();
                aggregateUtil.gather("strutProcess",
                    bitmaps,
                    context,
                    answer,
                    pivotFieldId,
                    100,
                    false,
                    false,
                    Optional.absent(),
                    solutionLog,
                    (lastId, termId, count) -> rescorable.add(new TermIdLastIdCount(termId, lastId, count)),
                    stackBuffer);
                rescore(catwalkId,
                    modelId,
                    catwalkDefinition.catwalkQuery,
                    catwalkDefinition.featureScalars,
                    catwalkDefinition.featureStrategy,
                    false,
                    catwalkDefinition.numeratorScalars,
                    catwalkDefinition.numeratorStrategy,
                    handle,
                    rescorable,
                    pivotFieldId,
                    asyncConstrainFeature,
                    termScoreCache,
                    nilTermCache,
                    termFeatureCache,
                    new AtomicInteger(),
                    solutionLog);
                nilTermCache.setLastId(modelIdBytes, activityIndexLastId);
                LOG.inc("process>count", rescorable.size());
                LOG.inc("process>batch>pow>" + FilerIO.chunkPower(rescorable.size(), 0));
            } else {
                LOG.inc("process>skip");
            }
        }
    }

    <BM extends IBM, IBM> LastIdCacheKeyValues getTermScoreCache(MiruRequestContext<BM, IBM, ? extends MiruSipCursor<?>> context,
        String catwalkId) {
        int payloadSize = -1; // TODO fix maybe? this is amazing
        return context.getCacheProvider().getLastIdKeyValues("strut-scores-" + catwalkId, payloadSize, false, maxHeapPressureInBytes, "cuckoo",
            hashIndexLoadFactor);
    }

    <BM extends IBM, IBM> CacheKeyBitmaps<BM, IBM> getNilTermCache(MiruRequestContext<BM, IBM, ? extends MiruSipCursor<?>> context,
        String catwalkId) {
        int payloadSize = -1; // TODO fix maybe? this is amazing
        return context.getCacheProvider().getCacheKeyBitmaps("strut-nil-" + catwalkId, payloadSize, maxHeapPressureInBytes, "cuckoo",
            hashIndexLoadFactor);
    }

    <BM extends IBM, IBM> TimestampedCacheKeyValues getTermFeatureCache(MiruRequestContext<BM, IBM, ? extends MiruSipCursor<?>> context, String catwalkId) {
        int payloadSize = 4; // this is amazing
        return context.getCacheProvider().getTimestampedKeyValues("strut-features-" + catwalkId, payloadSize, false, maxHeapPressureInBytes, "cuckoo", 0d);
    }

    private <BM extends IBM, IBM> List<Scored> rescore(
        String catwalkId,
        String modelId,
        CatwalkQuery catwalkQuery,
        float[] featureScalars,
        Strategy featureStrategy,
        boolean includeFeatures,
        float[] numeratorScalars,
        Strategy numeratorStrategy,
        MiruRequestHandle<BM, IBM, ?> handle,
        List<TermIdLastIdCount> score,
        int pivotFieldId,
        BM[] constrainFeature,
        LastIdCacheKeyValues termScoreCache,
        CacheKeyBitmaps<BM, IBM> nilTermCache,
        TimestampedCacheKeyValues termFeatureCache,
        AtomicInteger totalPartitionCount,
        MiruSolutionLog solutionLog) throws Exception {

        long startStrut = System.currentTimeMillis();
        MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();
        MiruRequestContext<BM, IBM, ?> context = handle.getRequestContext();
        MiruPartitionCoord coord = handle.getCoord();
        MiruFieldIndex<BM, IBM> primaryIndex = context.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);

        StackBuffer stackBuffer = new StackBuffer();

        int[] scoredToLastIds = new int[score.size()];
        Arrays.fill(scoredToLastIds, -1);
        List<Scored> results = Lists.newArrayList();
        List<Scored> updates = Lists.newArrayList();

        strut.yourStuff("strut",
            coord,
            bitmaps,
            context,
            catwalkId,
            modelId,
            catwalkQuery,
            featureScalars,
            featureStrategy,
            includeFeatures,
            numeratorScalars,
            numeratorStrategy,
            topNValuesPerFeature,
            termFeatureCache,
            (streamBitmaps) -> {
                TermIdLastIdCount[] rescoreMiruTermIds = score.toArray(new TermIdLastIdCount[0]);
                MiruTermId[] miruTermIds = new MiruTermId[rescoreMiruTermIds.length];
                for (int i = 0; i < rescoreMiruTermIds.length; i++) {
                    miruTermIds[i] = rescoreMiruTermIds[i].termId;
                }

                BM[][] answers = bitmaps.createMultiArrayOf(score.size(), constrainFeature.length);
                bitmaps.multiTx(
                    (tx, stackBuffer1) -> primaryIndex.multiTxIndex("strut", pivotFieldId, miruTermIds, -1, stackBuffer1, tx),
                    (index, lastId, bitmap) -> {
                        for (int i = 0; i < constrainFeature.length; i++) {
                            if (constrainFeature[i] != null) {
                                answers[index][i] = bitmaps.and(Arrays.asList(bitmap, constrainFeature[i]));
                            } else {
                                answers[index][i] = bitmap;
                            }
                        }
                        scoredToLastIds[index] = lastId;
                    },
                    stackBuffer);

                for (int i = 0; i < rescoreMiruTermIds.length; i++) {
                    if (!streamBitmaps.stream(i, rescoreMiruTermIds[i].lastId, pivotFieldId, rescoreMiruTermIds[i].termId, scoredToLastIds[i], answers[i])) {
                        return false;
                    }
                }
                return true;
            },
            (streamIndex, hotness, cacheable) -> {
                results.add(hotness);
                if (cacheable) {
                    updates.add(hotness);
                }
                return true;
            },
            totalPartitionCount,
            solutionLog);
        solutionLog.log(MiruSolutionLogLevel.INFO, "Strut rescore took {} ms", System.currentTimeMillis() - startStrut);

        if (!updates.isEmpty()) {
            long startOfUpdates = System.currentTimeMillis();
            commitAndNil(modelId, bitmaps, context, pivotFieldId, termScoreCache, nilTermCache, updates, stackBuffer);
            if (shareScores) {
                shareOut(coord, new StrutShare(coord.tenantId, coord.partitionId, catwalkQuery, modelId, pivotFieldId, updates));
            }
            long totalTimeScoreUpdates = System.currentTimeMillis() - startOfUpdates;
            LOG.info("Strut score updates {} features in {} ms for {}", updates.size(), totalTimeScoreUpdates, coord);
            solutionLog.log(MiruSolutionLogLevel.INFO, "Strut score updates {} features in {} ms", updates.size(), totalTimeScoreUpdates);
        }
        return results;
    }

    private <BM extends IBM, IBM> BM[] buildConstrainFeatures(MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> context,
        CatwalkQuery catwalkQuery,
        int activityIndexLastId,
        StackBuffer stackBuffer,
        MiruSolutionLog solutionLog) throws Exception {

        CatwalkQuery.CatwalkFeature[] features = catwalkQuery.features;
        BM[] constrainFeature = bitmaps.createArrayOf(features.length);
        for (int i = 0; i < features.length; i++) {
            if (catwalkQuery.features[i] != null && !MiruFilter.NO_FILTER.equals(catwalkQuery.features[i].featureFilter)) {
                BM constrained = aggregateUtil.filter("strutCatwalk",
                    bitmaps,
                    context,
                    catwalkQuery.features[i].featureFilter,
                    solutionLog,
                    null,
                    activityIndexLastId,
                    -1,
                    -1,
                    stackBuffer);
                constrainFeature[i] = constrained;
            }
        }

        return constrainFeature;
    }

    private static class StrutQueueKey {

        public final MiruPartitionCoord coord;
        public final String catwalkId;
        public final String modelId;
        public final int pivotFieldId;

        public StrutQueueKey(MiruPartitionCoord coord, String catwalkId, String modelId, int pivotFieldId) {
            this.coord = coord;
            this.catwalkId = catwalkId;
            this.modelId = modelId;
            this.pivotFieldId = pivotFieldId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            StrutQueueKey that = (StrutQueueKey) o;

            if (pivotFieldId != that.pivotFieldId) {
                return false;
            }
            if (coord != null ? !coord.equals(that.coord) : that.coord != null) {
                return false;
            }
            if (catwalkId != null ? !catwalkId.equals(that.catwalkId) : that.catwalkId != null) {
                return false;
            }
            return !(modelId != null ? !modelId.equals(that.modelId) : that.modelId != null);

        }

        @Override
        public int hashCode() {
            int result = coord != null ? coord.hashCode() : 0;
            result = 31 * result + (catwalkId != null ? catwalkId.hashCode() : 0);
            result = 31 * result + (modelId != null ? modelId.hashCode() : 0);
            result = 31 * result + pivotFieldId;
            return result;
        }
    }

    private static class CatwalkDefinition {

        final String catwalkId;
        final CatwalkQuery catwalkQuery;
        final float[] numeratorScalars;
        final Strategy numeratorStrategy;
        final float[] featureScalars;
        final Strategy featureStrategy;

        public CatwalkDefinition(String catwalkId,
            CatwalkQuery catwalkQuery,
            float[] numeratorScalars,
            Strategy numeratorStrategy,
            float[] featureScalars,
            Strategy featureStrategy) {
            this.catwalkId = catwalkId;
            this.catwalkQuery = catwalkQuery;
            this.numeratorScalars = numeratorScalars;
            this.numeratorStrategy = numeratorStrategy;
            this.featureScalars = featureScalars;
            this.featureStrategy = featureStrategy;
        }
    }

}
