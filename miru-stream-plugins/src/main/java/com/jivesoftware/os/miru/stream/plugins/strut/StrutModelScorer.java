package com.jivesoftware.os.miru.stream.plugins.strut;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.CacheKeyValues;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruAggregateUtil;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkQuery;
import com.jivesoftware.os.miru.stream.plugins.strut.Strut.Scored;
import com.jivesoftware.os.miru.stream.plugins.strut.StrutQuery.Strategy;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    private final MiruProvider miruProvider;
    private final Strut strut;
    private final MiruAggregateUtil aggregateUtil;
    private final AtomicLong pendingUpdates;
    private final int maxUpdatesBeforeFlush;

    private final Map<String, CatwalkDefinition> catwalks = Maps.newConcurrentMap();
    private final LinkedHashMap<StrutQueueKey, Set<MiruTermId>>[] queues;

    private final AtomicBoolean running = new AtomicBoolean();
    private final List<Future<?>> futures = Lists.newArrayList();

    public StrutModelScorer(MiruProvider miruProvider,
        Strut strut,
        MiruAggregateUtil aggregateUtil,
        AtomicLong pendingUpdates,
        int maxUpdatesBeforeFlush,
        int queueStripeCount) {
        this.miruProvider = miruProvider;
        this.strut = strut;
        this.aggregateUtil = aggregateUtil;
        this.pendingUpdates = pendingUpdates;
        this.maxUpdatesBeforeFlush = maxUpdatesBeforeFlush;

        this.queues = new LinkedHashMap[queueStripeCount];
        for (int i = 0; i < queueStripeCount; i++) {
            queues[i] = Maps.newLinkedHashMap();
        }
    }

    public void start(ScheduledExecutorService executorService, int queueStripeCount, long consumeIntervalMillis) {
        running.set(true);
        for (int i = 0; i < queueStripeCount; i++) {
            LinkedHashMap<StrutQueueKey, Set<MiruTermId>> queue = queues[i];
            futures.add(executorService.schedule(() -> {
                consume(queue);
                return null;
            }, consumeIntervalMillis, TimeUnit.MILLISECONDS));
        }
    }

    public void stop() {
        running.set(false);
        for (Future<?> future : futures) {
            future.cancel(true);
        }
    }

    static void score(String modelId,
        int numeratorsCount,
        MiruTermId[] termIds,
        CacheKeyValues cacheKeyValues,
        ScoredStream scoredStream,
        StackBuffer stackBuffer) throws Exception {

        byte[][] keys = new byte[termIds.length][];
        for (int i = 0; i < keys.length; i++) {
            if (termIds[i] != null) {
                keys[i] = termIds[i].getBytes();
            }
        }
        cacheKeyValues.get(modelId, keys, (index, key, value) -> {
            float[] scores = new float[numeratorsCount];
            int lastId;
            if (value != null && value.length == (4 * numeratorsCount + 4)) {
                int offset = 0;
                for (int i = 0; i < numeratorsCount; i++) {
                    scores[i] = FilerIO.bytesFloat(value, offset);
                    offset += 4;
                }
                lastId = FilerIO.bytesInt(value, offset);
            } else {
                if (value != null) {
                    LOG.warn("Ignored strut model score with invalid length {}", value.length);
                }
                Arrays.fill(scores, Float.NaN);
                lastId = -1;
            }
            return scoredStream.score(index, scores, lastId);
        }, stackBuffer);
    }

    static void commit(String modelId,
        CacheKeyValues cacheKeyValues,
        List<Strut.Scored> updates,
        StackBuffer stackBuffer) throws Exception {

        byte[][] keys = new byte[updates.size()][];
        byte[][] values = new byte[updates.size()][];
        for (int i = 0; i < keys.length; i++) {
            Scored update = updates.get(i);
            byte[] payload = new byte[4 * update.scores.length + 4];
            int offset = 0;
            for (int j = 0; j < update.scores.length; j++) {
                byte[] scoreBytes = FilerIO.floatBytes(update.scores[j]);
                System.arraycopy(scoreBytes, 0, payload, offset, 4);
                offset += 4;
            }

            FilerIO.intBytes(update.scoredToLastId, payload, offset);

            keys[i] = update.term.getBytes();
            values[i] = payload;
        }
        cacheKeyValues.put(modelId, keys, values, false, false, stackBuffer);
    }

    void enqueue(MiruPartitionCoord coord, StrutQuery strutQuery, int pivotFieldId, List<LastIdAndTermId> lastIdAndTermIds) {
        catwalks.computeIfAbsent(strutQuery.catwalkId, key -> new CatwalkDefinition(strutQuery.catwalkId,
            strutQuery.catwalkQuery,
            strutQuery.numeratorScalars,
            strutQuery.numeratorStrategy,
            strutQuery.featureScalars,
            strutQuery.featureStrategy,
            strutQuery.featureFilter));

        StrutQueueKey key = new StrutQueueKey(coord, strutQuery.catwalkId, strutQuery.modelId, pivotFieldId);
        int stripe = Math.abs(key.hashCode() % queues.length);
        int[] count = { 0 };
        synchronized (queues[stripe]) {
            queues[stripe].compute(key, (key1, existing) -> {
                if (existing == null) {
                    existing = Sets.newHashSet();
                }
                for (LastIdAndTermId lastIdAndTermId : lastIdAndTermIds) {
                    if (existing.add(lastIdAndTermId.termId)) {
                        count[0]++;
                    }
                }
                return existing;
            });
        }
        pendingUpdates.addAndGet(count[0]);
    }

    private void consume(LinkedHashMap<StrutQueueKey, Set<MiruTermId>> queue) {
        LOG.inc("strut>scorer>runs");
        StackBuffer stackBuffer = new StackBuffer();
        MiruSolutionLog solutionLog = new MiruSolutionLog(MiruSolutionLogLevel.NONE);
        try {
            while (!queue.isEmpty() && running.get()) {
                Entry<StrutQueueKey, Set<MiruTermId>> entry = null;
                synchronized (queue) {
                    Iterator<Entry<StrutQueueKey, Set<MiruTermId>>> iter = queue.entrySet().iterator();
                    if (iter.hasNext()) {
                        entry = iter.next();
                        iter.remove();
                    }
                }

                if (entry != null) {
                    LOG.inc("strut>scorer>consumed");
                    StrutModelScorer.StrutQueueKey key = entry.getKey();
                    Set<MiruTermId> termIds = entry.getValue();
                    int count = termIds.size();
                    try {
                        Optional<? extends MiruQueryablePartition<?, ?>> optionalQueryablePartition = miruProvider.getMiru(key.coord.tenantId)
                            .getQueryablePartition(key.coord);
                        if (optionalQueryablePartition.isPresent()) {
                            MiruQueryablePartition<?, ?> replica = optionalQueryablePartition.get();

                            try {
                                process((MiruQueryablePartition) replica, key.catwalkId, key.modelId, key.pivotFieldId, termIds, stackBuffer, solutionLog);
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
        } catch (Throwable t) {
            LOG.error("Failure while consuming strut model queue", t);
        }
    }

    private <BM extends IBM, IBM> void process(MiruQueryablePartition<BM, IBM> replica,
        String catwalkId,
        String modelId,
        int pivotFieldId,
        Collection<MiruTermId> termIds,
        StackBuffer stackBuffer,
        MiruSolutionLog solutionLog) throws Exception {

        CatwalkDefinition catwalkDefinition = catwalks.get(catwalkId);
        if (catwalkDefinition == null) {
            LOG.warn("Ignored strut rescore of nonexistent catwalkId:{} for modelId:{} pivotFieldId:{} termCount:{}", catwalkId, modelId, termIds.size());
            return;
        }

        try (MiruRequestHandle<BM, IBM, ?> handle = replica.acquireQueryHandle()) {
            MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();
            MiruRequestContext<BM, IBM, ? extends MiruSipCursor<?>> context = handle.getRequestContext();

            CacheKeyValues cacheStores = getCacheKeyValues(context, catwalkId, catwalkDefinition.catwalkQuery);
            int activityIndexLastId = context.getActivityIndex().lastId(stackBuffer);

            List<LastIdAndTermId> asyncRescore = Lists.newArrayListWithCapacity(termIds.size());
            for (MiruTermId termId : termIds) {
                asyncRescore.add(new LastIdAndTermId(-1, termId));
            }

            BM[] asyncConstrainFeature = buildConstrainFeatures(bitmaps,
                context,
                catwalkDefinition.featureFilter,
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
                cacheStores,
                new AtomicInteger(),
                solutionLog);
        }
    }

    <BM extends IBM, IBM> CacheKeyValues getCacheKeyValues(MiruRequestContext<BM, IBM, ? extends MiruSipCursor<?>> context,
        String catwalkId,
        CatwalkQuery catwalkQuery) {
        int payloadSize = 4 * catwalkQuery.gatherFilters.length + 4; // this is amazing
        return context.getCacheProvider().get("strut-" + catwalkId, payloadSize, false, maxUpdatesBeforeFlush);
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
        CacheKeyValues cacheStores,
        AtomicInteger totalPartitionCount,
        MiruSolutionLog solutionLog) throws Exception {

        long startStrut = System.currentTimeMillis();
        MiruBitmaps<BM, IBM> bitmaps = handle.getBitmaps();
        MiruRequestContext<BM, IBM, ?> context = handle.getRequestContext();
        MiruFieldIndex<BM, IBM> primaryIndex = context.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);

        StackBuffer stackBuffer = new StackBuffer();

        int[] scoredToLastIds = new int[score.size()];
        Arrays.fill(scoredToLastIds, -1);
        List<Scored> results = Lists.newArrayList();
        List<Scored> updates = Lists.newArrayList();

        strut.yourStuff("strut",
            handle.getCoord(),
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
                    if (!streamBitmaps.stream(i, rescoreMiruTermIds[i].lastId, rescoreMiruTermIds[i].termId, scoredToLastIds[i], answers[i])) {
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
            commit(modelId, cacheStores, updates, stackBuffer);
            long totalTimeScoreUpdates = System.currentTimeMillis() - startOfUpdates;
            LOG.info("Strut score updates {} features in {} ms", updates.size(), totalTimeScoreUpdates);
            solutionLog.log(MiruSolutionLogLevel.INFO, "Strut score updates {} features in {} ms", updates.size(), totalTimeScoreUpdates);
        }
        return results;
    }

    <BM extends IBM, IBM> BM[] buildConstrainFeatures(MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> context,
        MiruFilter featureFilter,
        CatwalkQuery catwalkQuery,
        int activityIndexLastId,
        StackBuffer stackBuffer,
        MiruSolutionLog solutionLog) throws Exception {

        MiruSchema schema = context.getSchema();
        MiruTermComposer termComposer = context.getTermComposer();

        BM strutFeature = null;
        if (!MiruFilter.NO_FILTER.equals(featureFilter)) {
            strutFeature = aggregateUtil.filter("strutFeature",
                bitmaps,
                schema,
                termComposer,
                context.getFieldIndexProvider(),
                featureFilter,
                solutionLog,
                null,
                activityIndexLastId,
                -1,
                stackBuffer);
        }

        CatwalkQuery.CatwalkFeature[] features = catwalkQuery.features;
        BM[] constrainFeature = bitmaps.createArrayOf(features.length);
        for (int i = 0; i < features.length; i++) {
            List<IBM> constrainAnds = Lists.newArrayList();
            if (catwalkQuery.features[i] != null) {
                constrainAnds.add(aggregateUtil.filter("strutCatwalk",
                    bitmaps,
                    schema,
                    termComposer,
                    context.getFieldIndexProvider(),
                    catwalkQuery.features[i].featureFilter,
                    solutionLog,
                    null,
                    activityIndexLastId,
                    -1,
                    stackBuffer));
            }
            if (strutFeature != null) {
                constrainAnds.add(strutFeature);
            }
            constrainFeature[i] = bitmaps.and(constrainAnds);
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
        final MiruFilter featureFilter;

        public CatwalkDefinition(String catwalkId,
            CatwalkQuery catwalkQuery,
            float[] numeratorScalars,
            Strategy numeratorStrategy,
            float[] featureScalars,
            Strategy featureStrategy,
            MiruFilter featureFilter) {
            this.catwalkId = catwalkId;
            this.catwalkQuery = catwalkQuery;
            this.numeratorScalars = numeratorScalars;
            this.numeratorStrategy = numeratorStrategy;
            this.featureScalars = featureScalars;
            this.featureStrategy = featureStrategy;
            this.featureFilter = featureFilter;
        }
    }

    static class LastIdAndTermId {

        public final int lastId;
        public final MiruTermId termId;

        public LastIdAndTermId(int lastId, MiruTermId termId) {
            this.lastId = lastId;
            this.termId = termId;
        }
    }
}
