package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.LastIdCacheKeyValues;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.TimestampedCacheKeyValues;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.miru.plugin.partition.OrderedPartitions;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkQuery;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutQuery.Strategy;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
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

    private final MiruProvider<? extends Miru> miruProvider;
    private final Strut strut;
    private final StrutRemotePartition strutRemotePartition;
    private final MiruAggregateUtil aggregateUtil;
    private final AtomicLong pendingUpdates;
    private final int topNValuesPerFeature;
    private final long maxHeapPressureInBytes;
    private final double hashIndexLoadFactor;
    private final boolean shareScores;

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
        boolean shareScores) {
        this.miruProvider = miruProvider;
        this.strut = strut;
        this.strutRemotePartition = strutRemotePartition;
        this.aggregateUtil = aggregateUtil;
        this.pendingUpdates = pendingUpdates;
        this.topNValuesPerFeature = topNValuesPerFeature;
        this.maxHeapPressureInBytes = maxHeapPressureInBytes;
        this.hashIndexLoadFactor = hashIndexLoadFactor;
        this.shareScores = shareScores;

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

    static void score(String[] modelId,
        int numeratorsCount,
        MiruTermId[] termIds,
        final LastIdCacheKeyValues[] termScoreCaches,
        float[] termScoreCacheScalars,
        ScoredStream scoredStream,
        StackBuffer stackBuffer) throws Exception {

        byte[][] keys = new byte[termIds.length][];
        for (int i = 0; i < keys.length; i++) {
            if (termIds[i] != null) {
                keys[i] = termIds[i].getBytes();
            }
        }

        float[][] scores = new float[termIds.length][numeratorsCount];
        int[] lastIds = new int[termIds.length];

        float sumOfScalars = 0;
        for (int c = 0; c < termScoreCacheScalars.length; c++) {
            LastIdCacheKeyValues termScoreCache = termScoreCaches[c];
            float termScoreCacheScalar = termScoreCacheScalars[c];
            sumOfScalars += termScoreCacheScalar;
            termScoreCache.get(modelId[c].getBytes(StandardCharsets.UTF_8), keys, (index, value, lastId) -> {
                if (value != null && value.capacity() == (4 * numeratorsCount)) {
                    int offset = 0;
                    for (int n = 0; n < numeratorsCount; n++) {
                        scores[index][n] += (value.getFloat(offset) * termScoreCacheScalar);
                        offset += 4;
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

        for (int i = 0; i < termIds.length; i++) {
            for (int n = 0; n < numeratorsCount; n++) {
                scores[i][n] /= sumOfScalars;
            }
            if (!scoredStream.score(i, scores[i], lastIds[i])) {
                return;
            }
        }
    }

    static void commit(String modelId,
        LastIdCacheKeyValues termScoreCache,
        List<Scored> updates,
        StackBuffer stackBuffer) throws Exception {

        termScoreCache.put(modelId.getBytes(StandardCharsets.UTF_8),
            false,
            false,
            stream -> {
                for (Scored update : updates) {
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
                }
                return true;
            },
            stackBuffer);
    }

    void shareOut(MiruPartitionCoord coord, StrutShare share) throws Exception {
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
            LastIdCacheKeyValues termScoreCache = getTermScoreCache(context, share.catwalkQuery.catwalkId);
            commit(share.modelId, termScoreCache, share.updates, new StackBuffer());
        } catch (Exception e) {
            LOG.warn("Failed to commit shared strut updates for {}", new Object[] { coord }, e);
        }
    }

    void enqueue(MiruPartitionCoord coord, StrutQuery strutQuery, int pivotFieldId, List<MiruTermId> termIds) {

        for (StrutModelScalar modelScalar : strutQuery.modelScalars) {

            CatwalkDefinition catwalkDefinition = new CatwalkDefinition(modelScalar.catwalkId,
                modelScalar.catwalkQuery,
                strutQuery.numeratorScalars,
                strutQuery.numeratorStrategy,
                strutQuery.featureScalars,
                strutQuery.featureStrategy);

            StrutQueueKey key = new StrutQueueKey(coord, modelScalar.catwalkId, modelScalar.modelId, pivotFieldId);
            int stripe = Math.abs(key.hashCode() % queues.length);
            int[] count = { 0 };
            synchronized (queues[stripe]) {
                queues[stripe].compute(key, (key1, existing) -> {
                    if (existing == null) {
                        existing = new Enqueued(Sets.newHashSet(), catwalkDefinition);
                    }
                    int beforeCount = existing.termIds.size();
                    existing.termIds.addAll(termIds);
                    count[0] += existing.termIds.size() - beforeCount;
                    return existing;
                });
            }
            pendingUpdates.addAndGet(count[0]);
        }
    }

    static class Enqueued {

        final Set<MiruTermId> termIds;
        final CatwalkDefinition catwalkDefinition;

        public Enqueued(Set<MiruTermId> termIds, CatwalkDefinition catwalkDefinition) {
            this.termIds = termIds;
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
                int count = enqueued.termIds.size();
                try {
                    Optional<? extends MiruQueryablePartition<?, ?>> optionalQueryablePartition = miruProvider.getMiru(key.coord.tenantId)
                        .getQueryablePartition(key.coord);
                    if (optionalQueryablePartition.isPresent()) {
                        MiruQueryablePartition<?, ?> replica = optionalQueryablePartition.get();

                        try {
                            process((MiruQueryablePartition) replica, key.catwalkId, key.modelId, key.pivotFieldId, enqueued.catwalkDefinition,
                                enqueued.termIds, stackBuffer, solutionLog);
                            LOG.inc("strut>scorer>processed", count);
                        } catch (Exception e) {
                            LOG.inc("strut>scorer>failed", count);
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Failed to consume catwalkId:{} modelId:{} pivotFieldId:{} termCount:{}",
                                    new Object[] { key.catwalkId, key.modelId, key.pivotFieldId, count }, e);
                            } else {
                                LOG.warn("Failed to consume catwalkId:{} modelId:{} pivotFieldId:{} termCount:{} message:{}",
                                    key.catwalkId, key.modelId, key.pivotFieldId, count, e.getMessage());
                            }
                        }
                    } else {
                        LOG.inc("strut>scorer>ignored", count);
                    }
                } finally {
                    pendingUpdates.addAndGet(-count);
                }
            }
        }
    }

    private <BM extends IBM, IBM> void process(MiruQueryablePartition<BM, IBM> replica,
        String catwalkId,
        String modelId,
        int pivotFieldId,
        CatwalkDefinition catwalkDefinition,
        Collection<MiruTermId> termIds,
        StackBuffer stackBuffer,
        MiruSolutionLog solutionLog) throws Exception {

        try (MiruRequestHandle<BM, IBM, ?> handle = replica.acquireQueryHandle()) {
            MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();
            MiruRequestContext<BM, IBM, ? extends MiruSipCursor<?>> context = handle.getRequestContext();

            LastIdCacheKeyValues termScoreCache = getTermScoreCache(context, catwalkDefinition.catwalkQuery.catwalkId);
            TimestampedCacheKeyValues termFeatureCache = getTermFeatureCache(context, catwalkId);
            int activityIndexLastId = context.getActivityIndex().lastId(stackBuffer);

            List<LastIdAndTermId> asyncRescore = Lists.newArrayListWithCapacity(termIds.size());
            for (MiruTermId termId : termIds) {
                asyncRescore.add(new LastIdAndTermId(-1, termId, -1));
            }

            BM[] asyncConstrainFeature = buildConstrainFeatures(bitmaps,
                context,
                catwalkDefinition.catwalkQuery,
                activityIndexLastId,
                stackBuffer,
                solutionLog);
            rescore(catwalkId,
                modelId,
                catwalkDefinition.catwalkQuery,
                catwalkDefinition.featureScalars,
                catwalkDefinition.featureStrategy,
                false,
                catwalkDefinition.numeratorScalars,
                catwalkDefinition.numeratorStrategy,
                handle,
                asyncRescore,
                pivotFieldId,
                asyncConstrainFeature,
                termScoreCache,
                termFeatureCache,
                new AtomicInteger(),
                solutionLog);
        }
    }

    <BM extends IBM, IBM> LastIdCacheKeyValues getTermScoreCache(MiruRequestContext<BM, IBM, ? extends MiruSipCursor<?>> context,
        String catwalkId) {
        int payloadSize = -1; // TODO fix maybe? this is amazing
        return context.getCacheProvider().getLastIdKeyValues("strut-scores-" + catwalkId, payloadSize, false, maxHeapPressureInBytes, hashIndexLoadFactor);
    }

    <BM extends IBM, IBM> TimestampedCacheKeyValues getTermFeatureCache(MiruRequestContext<BM, IBM, ? extends MiruSipCursor<?>> context, String catwalkId) {
        int payloadSize = 4; // this is amazing
        return context.getCacheProvider().getTimestampedKeyValues("strut-features-" + catwalkId, payloadSize, false, maxHeapPressureInBytes, 0d);
    }

    <BM extends IBM, IBM> List<Scored> rescore(
        String catwalkId,
        String modelId,
        CatwalkQuery catwalkQuery,
        float[] featureScalars,
        Strategy featureStrategy,
        boolean includeFeatures,
        float[] numeratorScalars,
        Strategy numeratorStrategy,
        MiruRequestHandle<BM, IBM, ?> handle,
        List<LastIdAndTermId> score,
        int pivotFieldId,
        BM[] constrainFeature,
        LastIdCacheKeyValues termScoreCache,
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
                LastIdAndTermId[] rescoreMiruTermIds = score.toArray(new LastIdAndTermId[0]);
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
            commit(modelId, termScoreCache, updates, stackBuffer);
            if (shareScores) {
                shareOut(coord, new StrutShare(coord.tenantId, coord.partitionId, catwalkQuery, modelId, updates));
            }
            long totalTimeScoreUpdates = System.currentTimeMillis() - startOfUpdates;
            LOG.info("Strut score updates {} features in {} ms for {}", updates.size(), totalTimeScoreUpdates, coord);
            solutionLog.log(MiruSolutionLogLevel.INFO, "Strut score updates {} features in {} ms", updates.size(), totalTimeScoreUpdates);
        }
        return results;
    }

    <BM extends IBM, IBM> BM[] buildConstrainFeatures(MiruBitmaps<BM, IBM> bitmaps,
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

    static class LastIdAndTermId {

        public final int lastId;
        public final MiruTermId termId;
        public final long count;

        public LastIdAndTermId(int lastId, MiruTermId termId, long count) {
            this.lastId = lastId;
            this.termId = termId;
            this.count = count;
        }
    }
}
