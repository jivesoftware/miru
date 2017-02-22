package com.jivesoftware.os.miru.plugin.solution;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;
import com.jivesoftware.os.filer.io.ByteArrayFiler;
import com.jivesoftware.os.filer.io.ByteBufferBackedFiler;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.KeyRange;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruFieldFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruFilterOperation;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.cache.MiruPluginCacheProvider.TimestampedCacheKeyValues;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.FieldMultiTermTxIndex;
import com.jivesoftware.os.miru.plugin.index.LastIdAndTermIdStream;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndexProvider;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.MiruTxIndex;
import com.jivesoftware.os.miru.plugin.index.TermIdLastIdCountStream;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.marshall.api.UtilLexMarshaller;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 */
public class MiruAggregateUtil {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public interface StreamBitmaps<BM> {

        boolean stream(int streamIndex, int lastId, int fieldId, MiruTermId termId, int scoredToLastId, BM[] answers) throws Exception;
    }

    public interface ConsumeBitmaps<BM> {

        boolean consume(StreamBitmaps<BM> streamBitmaps) throws Exception;
    }

    public interface GetAllTermIds {

        MiruTermId[][] getAll(String name, int[] ids, int offset, int count, MiruFieldDefinition fieldDefinition, StackBuffer stackBuffer) throws Exception;
    }

    public <BM extends IBM, IBM> void gatherFeatures(String name,
        MiruPartitionCoord coord,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruSchema schema,
        GetAllTermIds getAllTermIds,
        TimestampedCacheKeyValues termFeatureCache,
        ConsumeBitmaps<BM> consumeAnswers,
        int[][] featureFieldIds,
        int topNValuesPerFeature,
        FeatureStream stream,
        MiruSolutionLog solutionLog,
        StackBuffer stackBuffer) throws Exception {

        Set<Integer> uniqueFieldIds = Sets.newHashSet();
        for (int i = 0; i < featureFieldIds.length; i++) {
            if (featureFieldIds[i] != null) {
                for (int j = 0; j < featureFieldIds[i].length; j++) {
                    uniqueFieldIds.add(featureFieldIds[i][j]);
                }
            }
        }

        int fieldCount = schema.fieldCount();
        MiruTermId[][][] fieldTerms = new MiruTermId[fieldCount][][];

        @SuppressWarnings("unchecked")
        Multiset<Feature>[] features = new Multiset[featureFieldIds.length];
        @SuppressWarnings("unchecked")
        MinMaxPriorityQueue<FeatureAndCount>[] featureHeaps = new MinMaxPriorityQueue[featureFieldIds.length];
        for (int i = 0; i < features.length; i++) {
            features[i] = HashMultiset.create();
            featureHeaps[i] = MinMaxPriorityQueue.maximumSize(topNValuesPerFeature).create();
        }

        String cacheName = null;
        @SuppressWarnings("unchecked")
        Set<Feature>[] gathered = termFeatureCache == null ? null : new Set[featureFieldIds.length];
        if (termFeatureCache != null) {
            cacheName = termFeatureCache.name();
            for (int i = 0; i < features.length; i++) {
                gathered[i] = Sets.newHashSet();
            }
        }

        int batchSize = 1;
        boolean[][] featuresContained = new boolean[batchSize][featureFieldIds.length];
        int[] ids = new int[batchSize];
        long start = System.currentTimeMillis();

        GatherFeatureMetrics metrics = new GatherFeatureMetrics();

        consumeAnswers.consume((streamIndex, lastId, answerFieldId, answerTermId, answerScoredLastId, answerBitmaps) -> {
            metrics.termCount++;

            for (int i = 0; i < features.length; i++) {
                features[i].clear();
                featureHeaps[i].clear();
            }

            if (termFeatureCache != null) {
                for (int i = 0; i < gathered.length; i++) {
                    gathered[i].clear();
                }
                byte[] cacheId = Bytes.concat(FilerIO.intBytes(answerFieldId), answerTermId.getBytes());
                synchronized (termFeatureCache.lock(cacheId)) {
                    boolean[] discardFeatures = { false };
                    int[] lastScoredId = { -1 };
                    termFeatureCache.rangeScan(cacheId, null, null, (key, value, timestamp) -> {
                        Feature feature = Feature.unpack(new ByteBufferBackedFiler(key), stackBuffer);
                        int featureId = feature.featureId;
                        if (featureId == -1) {
                            if (lastScoredId[0] != -1) {
                                LOG.error("Cache found multiple lastScoredIds for name:{} coord:{} cacheName:{} termId:{}",
                                    name, coord, termFeatureCache.name(), answerTermId);
                            }
                            lastScoredId[0] = (int) timestamp;
                        } else {
                            LOG.error("Cache was missing a lastScoredId for name:{} coord:{} cacheName:{} termId:{}",
                                name, coord, termFeatureCache.name(), answerTermId);
                        }
                        return false;
                    });

                    int fromId = lastScoredId[0] + 1;
                    metrics.minFromId = Math.min(metrics.minFromId, fromId);
                    metrics.maxFromId = Math.max(metrics.maxFromId, fromId);

                    if (answerScoredLastId >= fromId) {
                        gatherFeaturesForTerm(name, bitmaps, schema, featureFieldIds, stackBuffer, uniqueFieldIds, getAllTermIds, fieldTerms,
                            ids, featuresContained, answerBitmaps, features, gathered, fromId, answerScoredLastId, metrics);
                    }

                    termFeatureCache.rangeScan(cacheId, null, null, (key, value, timestamp) -> {
                        Feature feature = Feature.unpack(new ByteBufferBackedFiler(key), stackBuffer);
                        int featureId = feature.featureId;
                        if (featureId == -1) {
                            if (lastScoredId[0] != (int) timestamp) {
                                discardFeatures[0] = true;
                                LOG.warn("Found name:{} coord:{} cacheName:{} termId:{} feature:{} timestamp:{} which is more recent than lastScoredId:{}",
                                    name, coord, termFeatureCache.name(), answerTermId, feature, timestamp, lastScoredId[0]);
                                return false;
                            }
                            return true;
                        }

                        if (timestamp > lastScoredId[0]) {
                            discardFeatures[0] = true;
                            LOG.warn("Found name:{} coord:{} cacheName:{} termId:{} feature:{} timestamp:{} which is more recent than lastScoredId:{}",
                                name, coord, termFeatureCache.name(), answerTermId, feature, timestamp, lastScoredId[0]);
                            return false;
                        }

                        int count = value.getInt();
                        MiruTermId[] termIds = feature.termIds;
                        Feature f = new Feature(featureId, termIds);
                        int gatheredCount = features[featureId].count(f);
                        if (gatheredCount > 0) {
                            features[featureId].add(f, count);
                        }
                        featureHeaps[featureId].add(new FeatureAndCount(f, gatheredCount + count, timestamp));
                        gathered[featureId].remove(f);
                        metrics.cacheHitCount++;

                        return true;
                    });

                    if (discardFeatures[0]) {
                        for (int i = 0; i < features.length; i++) {
                            features[i].clear();
                            featureHeaps[i].clear();
                        }

                        fromId = 0;
                        gatherFeaturesForTerm(name, bitmaps, schema, featureFieldIds, stackBuffer, uniqueFieldIds, getAllTermIds, fieldTerms,
                            ids, featuresContained, answerBitmaps, features, null, fromId, answerScoredLastId, metrics);

                        for (int i = 0; i < features.length; i++) {
                            for (Entry<Feature> entry : features[i].entrySet()) {
                                Feature feature = entry.getElement();
                                int count = entry.getCount();
                                featureHeaps[i].add(new FeatureAndCount(feature, count, answerScoredLastId));
                            }
                        }
                    } else {
                        for (int i = 0; i < gathered.length; i++) {
                            for (Feature feature : gathered[i]) {
                                int count = features[i].count(feature);
                                featureHeaps[i].add(new FeatureAndCount(feature, count, answerScoredLastId));
                            }
                        }
                    }

                    boolean needToSave = false;
                    for (int i = 0; i < features.length; i++) {
                        if (!features[i].isEmpty()) {
                            needToSave = true;
                            break;
                        }
                    }
                    if (needToSave) {
                        termFeatureCache.put(cacheId, false, false, stream1 -> {
                            for (int i = 0; i < features.length; i++) {
                                for (Entry<Feature> entry : features[i].entrySet()) {
                                    metrics.cacheSaveCount++;
                                    Feature feature = entry.getElement();
                                    if (!stream1.stream(feature.pack(stackBuffer), FilerIO.intBytes(entry.getCount()), answerScoredLastId)) {
                                        return false;
                                    }
                                }
                            }
                            return stream1.stream(Feature.pack(-1, null, stackBuffer), new byte[0], answerScoredLastId);
                        }, stackBuffer);
                    }
                }
            } else {
                metrics.minFromId = -1;
                metrics.maxFromId = -1;

                gatherFeaturesForTerm(name, bitmaps, schema, featureFieldIds, stackBuffer, uniqueFieldIds, getAllTermIds, fieldTerms,
                    ids, featuresContained, answerBitmaps, features, null, 0, answerScoredLastId, metrics);
            }

            metrics.minToId = Math.min(metrics.minToId, answerScoredLastId);
            metrics.maxToId = Math.max(metrics.maxToId, answerScoredLastId);

            for (int i = 0; i < featureHeaps.length; i++) {
                for (FeatureAndCount featureAndCount : featureHeaps[i]) {
                    Feature feature = featureAndCount.feature;
                    metrics.featureCount++;
                    boolean result = stream.stream(streamIndex, lastId, answerFieldId, answerTermId, answerScoredLastId, feature.featureId, feature.termIds,
                        featureAndCount.count);
                    if (!result) {
                        return false;
                    }
                }
            }
            return stream.stream(streamIndex, lastId, answerFieldId, answerTermId, answerScoredLastId, -1, null, -1);
        });
        LOG.info("Gathered name:{} coord:{} features:{} terms:{} elapsed:{}" +
                " - cacheName:{} skipped:{} consumed:{} fromId:{}/{} toId:{}/{} cacheHits={} cacheSaves={}",
            name, coord, metrics.featureCount, metrics.termCount, System.currentTimeMillis() - start,
            cacheName,
            metrics.skippedCount, metrics.consumedCount,
            metrics.minFromId, metrics.maxFromId,
            metrics.minToId, metrics.maxToId,
            metrics.cacheHitCount, metrics.cacheSaveCount);
        solutionLog.log(MiruSolutionLogLevel.INFO, "Gathered name:{} features:{} terms:{} elapsed:{}" +
                " - cacheName:{} skipped:{} consumed:{} fromId:{}/{} toId:{}/{} cacheHits={} cacheSaves={}",
            name, metrics.featureCount, metrics.termCount, System.currentTimeMillis() - start,
            cacheName,
            metrics.skippedCount, metrics.consumedCount,
            metrics.minFromId, metrics.maxFromId,
            metrics.minToId, metrics.maxToId,
            metrics.cacheHitCount, metrics.cacheSaveCount);
    }

    private static class GatherFeatureMetrics {
        private long minFromId = Integer.MAX_VALUE;
        private long maxFromId = Integer.MIN_VALUE;
        private long minToId = Integer.MAX_VALUE;
        private long maxToId = Integer.MIN_VALUE;
        private int termCount;
        private int featureCount;
        private int skippedCount;
        private int consumedCount;
        private int cacheHitCount;
        private int cacheSaveCount;
    }

    private <BM extends IBM, IBM> void gatherFeaturesForTerm(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruSchema schema,
        int[][] featureFieldIds,
        StackBuffer stackBuffer,
        Set<Integer> uniqueFieldIds,
        GetAllTermIds getAllTermIds,
        MiruTermId[][][] fieldTerms,
        int[] ids,
        boolean[][] featuresContained,
        BM[] answerBitmaps,
        Multiset<Feature>[] features,
        Set<Feature>[] gathered,
        int fromIdInclusive,
        int toIdInclusive,
        GatherFeatureMetrics metrics) throws Exception {

        MiruIntIterator[] intIters = new MiruIntIterator[answerBitmaps.length];
        for (int i = 0; i < intIters.length; i++) {
            if (answerBitmaps[i] != null) {
                intIters[i] = bitmaps.intIterator(answerBitmaps[i]);
            }
        }

        CollatingIntIterator iter = new CollatingIntIterator(intIters, false);
        int count = 0;
        while (iter.hasNext()) {
            ids[count] = iter.next(featuresContained[count]);
            if (ids[count] < fromIdInclusive || ids[count] > toIdInclusive) {
                metrics.skippedCount++;
                continue;
            }
            metrics.consumedCount++;
            count++;
            if (count == ids.length) {
                gatherAndCountFeaturesForTerm(name, featureFieldIds, stackBuffer, uniqueFieldIds, getAllTermIds, fieldTerms,
                    ids, featuresContained, count, features, gathered, schema, metrics);
                count = 0;
            }
        }
        if (count > 0) {
            gatherAndCountFeaturesForTerm(name, featureFieldIds, stackBuffer, uniqueFieldIds, getAllTermIds, fieldTerms,
                ids, featuresContained, count, features, gathered, schema, metrics);
            count = 0;
        }
    }

    private void gatherAndCountFeaturesForTerm(String name,
        int[][] featureFieldIds,
        StackBuffer stackBuffer,
        Set<Integer> uniqueFieldIds,
        GetAllTermIds getAllTermIds,
        MiruTermId[][][] fieldTerms,
        int[] ids,
        boolean[][] contained,
        int count,
        Multiset<Feature>[] features,
        Set<Feature>[] gathered,
        MiruSchema schema,
        GatherFeatureMetrics metrics) throws Exception {

        int[] consumableIds = new int[ids.length];
        for (int fieldId : uniqueFieldIds) {
            System.arraycopy(ids, 0, consumableIds, 0, ids.length);
            fieldTerms[fieldId] = getAllTermIds.getAll(name, consumableIds, 0, count, schema.getFieldDefinition(fieldId), stackBuffer);
        }

        PermutationStream permutationStream = (fi, permuteTermIds) -> {
            Feature feature = new Feature(fi, permuteTermIds);
            features[fi].add(feature);
            if (gathered != null) {
                gathered[fi].add(feature);
            }
            return true;
        };

        done:
        for (int index = 0; index < count; index++) {
            NEXT_FEATURE:
            for (int i = 0; i < featureFieldIds.length; i++) {
                if (contained[index][i]) {
                    int[] fieldIds = featureFieldIds[i];

                    MiruTermId[][] termIds = new MiruTermId[fieldIds.length][];
                    for (int j = 0; j < fieldIds.length; j++) {
                        MiruTermId[] fieldTermIds = fieldTerms[fieldIds[j]][index];
                        if (fieldTermIds == null || fieldTermIds.length == 0) {
                            continue NEXT_FEATURE;
                        }
                        termIds[j] = fieldTermIds;
                    }

                    if (!permutate(i, fieldIds.length, termIds, permutationStream)) {
                        break done;
                    }
                }
            }
        }
    }

    interface PermutationStream {

        boolean permutation(int index, MiruTermId[] termIds) throws Exception;
    }

    boolean permutate(int index, int depth, MiruTermId[][] termIds, PermutationStream stream) throws Exception {
        MiruTermId[] feature = new MiruTermId[depth];
        for (int k = 0; k < depth; k++) {
            feature[k] = termIds[k][0];
        }

        int[] position = new int[depth];
        DONE:
        while (true) {
            MiruTermId[] copy = new MiruTermId[feature.length];
            System.arraycopy(feature, 0, copy, 0, feature.length);
            if (!stream.permutation(index, copy)) {
                return false;
            }

            int k;
            for (k = depth - 1; k >= 0; k--) {
                position[k]++;
                if (position[k] >= termIds[k].length) {
                    position[k] = 0;
                    feature[k] = termIds[k][position[k]];
                } else {
                    feature[k] = termIds[k][position[k]];
                    break;
                }
            }
            if (k < 0) {
                break;
            }
        }
        return true;
    }

    /*private <BM extends IBM, IBM, S extends MiruSipCursor<S>> void indexValueBitsGatherFeatures(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, S> requestContext,
        ConsumeBitmaps<BM> consumeAnswers,
        int[][] featureFieldIds,
        boolean dedupe,
        FeatureStream stream,
        MiruSolutionLog solutionLog,
        StackBuffer stackBuffer) throws Exception {

        MiruFieldIndex<BM, IBM> valueBitsIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.valueBits);

        Set<Integer> uniqueFieldIds = Sets.newHashSet();
        for (int i = 0; i < featureFieldIds.length; i++) {
            if (featureFieldIds[i] != null) {
                for (int j = 0; j < featureFieldIds[i].length; j++) {
                    uniqueFieldIds.add(featureFieldIds[i][j]);
                }
            }
        }

        long start = System.currentTimeMillis();
        int bitmapCount = 0;
        long bitmapBytes = 0;
        byte[][] valueBuffers = new byte[requestContext.getSchema().fieldCount()][];
        List<FieldBits<BM, IBM>> baseFieldBits = Lists.newArrayList();
        for (int fieldId : uniqueFieldIds) {
            List<MiruTermId> termIds = Lists.newArrayList();
            valueBitsIndex.streamTermIdsForField(name, fieldId, null,
                termId -> {
                    termIds.add(termId);
                    return true;
                },
                stackBuffer);
            bitmapCount += termIds.size();

            @SuppressWarnings("unchecked")
            BitmapAndLastId<BM>[] results = new BitmapAndLastId[termIds.size()];
            valueBitsIndex.multiGet(name, fieldId, termIds.toArray(new MiruTermId[0]), results, stackBuffer);

            BM[] featureBits = bitmaps.createArrayOf(results.length);
            for (int i = 0; i < results.length; i++) {
                BitmapAndLastId<BM> result = results[i];
                bitmapBytes += bitmaps.sizeInBytes(result.bitmap);
                featureBits[i] = result.bitmap;
            }

            int[] bits = new int[termIds.size()];
            int maxBit = -1;
            for (int i = 0; i < termIds.size(); i++) {
                bits[i] = ValueIndex.bytesShort(termIds.get(i).getBytes());
                maxBit = Math.max(maxBit, bits[i]);
            }

            if (maxBit != -1) {
                valueBuffers[fieldId] = new byte[(maxBit / 8) + 1];
                for (int i = 0; i < termIds.size(); i++) {
                    MiruTermId termId = termIds.get(i);
                    int bit = ValueIndex.bytesShort(termId.getBytes());
                    baseFieldBits.add(new FieldBits<>(fieldId, bit, featureBits[i], bitmaps.cardinality(featureBits[i])));
                }
            }
        }

        solutionLog.log(MiruSolutionLogLevel.INFO, "Setup field bits for fields:{} bitmaps:{} bytes:{} took {} ms",
            uniqueFieldIds.size(), bitmapCount, bitmapBytes, System.currentTimeMillis() - start);
        start = System.currentTimeMillis();

        consumeAnswers.consume((streamIndex, lastId, answerTermId, answerScoredLastId, answerBitmap) -> {
            List<FieldBits<BM, IBM>> fieldBits = Lists.newArrayList();
            for (FieldBits<BM, IBM> base : baseFieldBits) {
                BM bitmap;
                long cardinality;
                if (answerBitmap != null) {
                    bitmap = bitmaps.and(Arrays.asList(answerBitmap, base.bitmap));
                    cardinality = bitmaps.cardinality(answerBitmap);
                } else {
                    bitmap = bitmaps.copy(base.bitmap);
                    cardinality = base.cardinality;
                }
                fieldBits.add(new FieldBits<>(base.fieldId, base.bit, bitmap, cardinality));
            }

            @SuppressWarnings("unchecked")
            Set<Feature>[] quickDedupe = dedupe ? new Set[featureFieldIds.length] : null;
            if (dedupe) {
                for (int i = 0; i < quickDedupe.length; i++) {
                    quickDedupe[i] = Sets.newHashSet();
                }
            }

            byte[][] values = new byte[valueBuffers.length][];
            Collections.sort(fieldBits);
            done:
            while (!fieldBits.isEmpty()) {
                if (quickDedupe != null) {
                    for (Set<Feature> qd : quickDedupe) {
                        qd.clear();
                    }
                }

                FieldBits<BM, IBM> next = fieldBits.remove(0);

                MiruIntIterator iter = bitmaps.intIterator(next.bitmap);
                while (iter.hasNext()) {
                    for (byte[] valueBuffer : valueBuffers) {
                        if (valueBuffer != null) {
                            Arrays.fill(valueBuffer, (byte) 0);
                        }
                    }

                    int id = iter.next();
                    valueBuffers[next.fieldId][next.bit >>> 3] |= 1 << (next.bit & 0x07);
                    for (FieldBits<BM, IBM> fb : fieldBits) {
                        if (bitmaps.removeIfPresent(fb.bitmap, id)) {
                            fb.cardinality--;
                            valueBuffers[fb.fieldId][fb.bit >>> 3] |= 1 << (fb.bit & 0x07);
                        }
                    }

                    Arrays.fill(values, null);
                    for (int i = 0; i < valueBuffers.length; i++) {
                        byte[] valueBuffer = valueBuffers[i];
                        if (valueBuffer != null) {
                            byte[] value = ValueIndex.unpackValue(valueBuffer);
                            if (value != null) {
                                values[i] = value;
                            }
                        }
                    }

                    next:
                    for (int featureId = 0; featureId < featureFieldIds.length; featureId++) {
                        if (featureFieldIds[featureId] == null) {
                            continue;
                        }
                        int[] fieldIds = featureFieldIds[featureId];
                        // make sure we have all the parts for this feature
                        for (int i = 0; i < fieldIds.length; i++) {
                            if (values[fieldIds[i]] == null) {
                                continue next;
                            }
                        }

                        MiruTermId[] featureTermIds = new MiruTermId[fieldIds.length];
                        for (int i = 0; i < fieldIds.length; i++) {
                            int fieldId = fieldIds[i];
                            MiruTermId termId = new MiruTermId(values[fieldId]);
                            featureTermIds[i] = termId;
                        }

                        if (dedupe && !quickDedupe[featureId].add(new Feature(featureTermIds))) {
                            continue;
                        }

                        if (!stream.stream(streamIndex, lastId, answerTermId, answerScoredLastId, featureId, featureTermIds)) {
                            break done;
                        }
                    }
                }
                Collections.sort(fieldBits);
            }
            return true;
        });

        solutionLog.log(MiruSolutionLogLevel.INFO, "Gather value bits took {} ms", System.currentTimeMillis() - start);
        start = System.currentTimeMillis();
    }*/

    public interface FeatureStream {

        boolean stream(int streamIndex,
            int lastId,
            int answerFieldId,
            MiruTermId answerTermId,
            int answerScoredLastId,
            int featureId,
            MiruTermId[] termIds,
            int count) throws Exception;
    }

    private static class FeatureAndCount implements Comparable<FeatureAndCount> {
        public final Feature feature;
        public final int count;
        public final long timestamp;

        public FeatureAndCount(Feature feature, int count, long timestamp) {
            this.feature = feature;
            this.count = count;
            this.timestamp = timestamp;
        }

        @Override
        public int compareTo(FeatureAndCount o) {
            int c = Integer.compare(o.count, count); // descending
            if (c != 0) {
                return c;
            }
            return Long.compare(o.timestamp, timestamp); // descending
        }
    }

    public static class Feature {

        public final int featureId;
        public final MiruTermId[] termIds;

        private int hash = 0;

        public Feature(int featureId, MiruTermId[] termIds) {
            this.featureId = featureId;
            this.termIds = termIds;
        }

        public byte[] pack(StackBuffer stackBuffer) throws IOException {
            return pack(featureId, termIds, stackBuffer);
        }

        public static byte[] pack(int featureId, MiruTermId[] featureTermIds, StackBuffer stackBuffer) throws IOException {
            ByteArrayFiler byteArrayFiler = new ByteArrayFiler();
            FilerIO.write(byteArrayFiler, UtilLexMarshaller.intToLex(featureId), "featureId");
            FilerIO.writeInt(byteArrayFiler, featureTermIds == null ? -1 : featureTermIds.length, "featureTermIdCount", stackBuffer);
            if (featureTermIds != null) {
                for (int i = 0; i < featureTermIds.length; i++) {
                    FilerIO.writeByteArray(byteArrayFiler, featureTermIds[i].getBytes(), "featureTermId", stackBuffer);
                }
            }
            return byteArrayFiler.getBytes();
        }

        public static Feature unpack(Filer filer, StackBuffer stackBuffer) throws IOException {
            byte[] lexFeatureId = new byte[4];
            FilerIO.read(filer, lexFeatureId);
            int featureId = UtilLexMarshaller.intFromLex(lexFeatureId);
            int featureTermIdCount = FilerIO.readInt(filer, "featureTermIdCount", stackBuffer);
            MiruTermId[] featureTermIds = null;
            if (featureTermIdCount != -1) {
                featureTermIds = new MiruTermId[featureTermIdCount];
                for (int i = 0; i < featureTermIdCount; i++) {
                    featureTermIds[i] = new MiruTermId(FilerIO.readByteArray(filer, "featureTermId", stackBuffer));
                }
            }
            return new Feature(featureId, featureTermIds);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Feature feature = (Feature) o;

            return Arrays.equals(termIds, feature.termIds);

        }

        @Override
        public int hashCode() {
            if (hash == 0) {
                hash = termIds != null ? Arrays.hashCode(termIds) : 0;
            }
            return hash;
        }

        @Override
        public String toString() {
            return "Feature{" +
                "featureId=" + featureId +
                ", termIds=" + Arrays.toString(termIds) +
                '}';
        }
    }

    /*private static class FieldBits<BM extends IBM, IBM> implements Comparable<FieldBits<BM, IBM>> {

        private final int fieldId;
        private final int bit;
        private final BM bitmap;

        private long cardinality;

        public FieldBits(int fieldId, int bit, BM bitmap, long cardinality) {
            this.fieldId = fieldId;
            this.bit = bit;
            this.bitmap = bitmap;
            this.cardinality = cardinality;
        }

        @Override
        public int compareTo(FieldBits<BM, IBM> o) {
            int c = Long.compare(cardinality, o.cardinality);
            if (c != 0) {
                return c;
            }
            c = Integer.compare(fieldId, o.fieldId);
            if (c != 0) {
                return c;
            }
            return Integer.compare(bit, o.bit);
        }
    }*/

    public <BM extends IBM, IBM, S extends MiruSipCursor<S>> void stream(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, S> requestContext,
        BM initialAnswer,
        Optional<BM> optionalCounter,
        int pivotFieldId,
        int batchSize,
        String streamField,
        StackBuffer stackBuffer,
        CallbackStream<MiruTermCount> terms)
        throws Exception {

        boolean traceEnabled = LOG.isTraceEnabled();
        boolean debugEnabled = LOG.isDebugEnabled();
        MiruSchema schema = requestContext.getSchema();

        if (bitmaps.supportsInPlace()) {
            // don't mutate the original
            initialAnswer = bitmaps.copy(initialAnswer);
        }

        final AtomicLong bytesTraversed = new AtomicLong();
        if (debugEnabled) {
            bytesTraversed.addAndGet(bitmaps.sizeInBytes(initialAnswer));
        }

        BM[] answer = bitmaps.createArrayOf(1);
        answer[0] = initialAnswer;

        BM[] counter;
        if (optionalCounter.isPresent()) {
            counter = bitmaps.createArrayOf(1);
            counter[0] = optionalCounter.orNull();
        } else {
            counter = null;
        }

        MiruActivityIndex activityIndex = requestContext.getActivityIndex();
        MiruFieldIndex<BM, IBM> primaryFieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        int streamFieldId = schema.getFieldId(streamField);

        MutableLong beforeCount = new MutableLong(counter != null ? bitmaps.cardinality(counter[0]) : bitmaps.cardinality(answer[0]));
        LOG.debug("stream: streamField={} streamFieldId={} pivotFieldId={} beforeCount={}", streamField, streamFieldId, pivotFieldId, beforeCount);

        FieldMultiTermTxIndex<BM, IBM> multiTermTxIndex = new FieldMultiTermTxIndex<>(name, primaryFieldIndex, pivotFieldId, -1);
        Map<MiruTermId, MiruTermId[]> distincts = Maps.newHashMapWithExpectedSize(batchSize);

        int[] ids = new int[batchSize];
        while (!bitmaps.isEmpty(answer[0])) {
            MiruIntIterator intIterator = bitmaps.intIterator(answer[0]);
            int added = 0;
            Arrays.fill(ids, -1);
            while (intIterator.hasNext() && added < batchSize) {
                ids[added] = intIterator.next();
                added++;
            }

            int[] actualIds = new int[added];
            System.arraycopy(ids, 0, actualIds, 0, added);

            if (bitmaps.supportsInPlace()) {
                bitmaps.inPlaceAndNot(answer[0], bitmaps.createWithBits(actualIds));
                //TODO possibly buggy, need to reevaluate
                /*bitmaps.inPlaceRemoveRange(answer[0], actualIds[0], actualIds[actualIds.length - 1] + 1);*/
            } else {
                answer[0] = bitmaps.andNot(answer[0], bitmaps.createWithBits(actualIds));
                //TODO possibly buggy, need to reevaluate
                /*answer[0] = bitmaps.removeRange(answer[0], actualIds[0], actualIds[actualIds.length - 1] + 1);*/
            }


            MiruTermId[][] all = activityIndex.getAll(name, actualIds, schema.getFieldDefinition(pivotFieldId), stackBuffer);
            MiruTermId[][] streamAll;
            if (streamFieldId != pivotFieldId) {
                streamAll = activityIndex.getAll(name, actualIds, schema.getFieldDefinition(streamFieldId), stackBuffer);
            } else {
                streamAll = all;
            }
            distincts.clear();
            for (int i = 0; i < all.length; i++) {
                MiruTermId[] termIds = all[i];
                MiruTermId[] streamTermIds = streamAll[i];
                if (termIds != null && termIds.length > 0) {
                    for (int j = 0; j < termIds.length; j++) {
                        distincts.put(termIds[j], streamTermIds);
                    }
                }
            }

            MiruTermId[] termIds = distincts.keySet().toArray(new MiruTermId[distincts.size()]);
            long[] termCounts = new long[termIds.length];
            multiTermTxIndex.setTermIds(termIds);

            bitmaps.multiTx(
                (tx, stackBuffer1) -> primaryFieldIndex.multiTxIndex(name, pivotFieldId, termIds, -1, stackBuffer1, tx),
                (index, lastId, bitmap) -> {
                    if (bitmaps.supportsInPlace()) {
                        bitmaps.inPlaceAndNot(answer[0], bitmap);
                        if (counter != null) {
                            bitmaps.inPlaceAndNot(counter[0], bitmap);
                        }
                    } else {
                        answer[0] = bitmaps.andNot(answer[0], bitmap);
                        if (counter != null) {
                            counter[0] = bitmaps.andNot(counter[0], bitmap);
                        }
                    }
                    long afterCount = (counter != null) ? bitmaps.cardinality(counter[0]) : bitmaps.cardinality(answer[0]);
                    termCounts[index] = beforeCount.longValue() - afterCount;
                    beforeCount.setValue(afterCount);
                },
                stackBuffer);

            for (int i = 0; i < termIds.length; i++) {
                MiruTermCount termCount = new MiruTermCount(termIds[i], distincts.get(termIds[i]), termCounts[i]);
                if (terms.callback(termCount) != termCount) {
                    return;
                }
            }
        }
        terms.callback(null); // EOS
        LOG.debug("stream: bytesTraversed={}", bytesTraversed.longValue());
    }

    public <BM extends IBM, IBM, S extends MiruSipCursor<S>> void gatherParallel(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, S> requestContext,
        BM answer,
        int pivotFieldId,
        int batchSize,
        boolean includeCounts,
        int limit,
        Optional<BM> counter,
        MiruSolutionLog solutionLog,
        ExecutorService executorService,
        TermIdLastIdCountStream stream,
        StackBuffer stackBuffer) throws Exception {

        BM[] splitAnswers = bitmaps.split(answer);
        BM[] splitCounters = null;
        if (counter.isPresent()) {
            int[] keys = bitmaps.keys(answer);
            splitCounters = bitmaps.extract(counter.get(), keys);
        }
        List<Future<List<TermIdLastIdCount>>> futures = Lists.newArrayListWithCapacity(splitAnswers.length);
        for (int i = splitAnswers.length - 1; i >= 0; i--) {
            BM splitAnswer = splitAnswers[i];
            if (bitmaps.isEmpty(splitAnswer)) {
                futures.add(null);
            } else {
                Optional<BM> splitCounter = splitCounters == null || splitCounters[i] == null ? Optional.absent() : Optional.fromNullable(splitCounters[i]);
                futures.add(executorService.submit(() -> {
                    try {
                        List<TermIdLastIdCount> gatherSplit = Lists.newArrayList();
                        gatherActivityLookup(name,
                            bitmaps,
                            requestContext,
                            splitAnswer,
                            pivotFieldId,
                            batchSize,
                            false,
                            includeCounts,
                            splitCounter,
                            solutionLog,
                            (lastId, termId, count) -> {
                                gatherSplit.add(new TermIdLastIdCount(termId, lastId, count));
                                return true;
                            },
                            stackBuffer);
                        return gatherSplit;
                    } catch (InterruptedException e) {
                        // ignore
                        return null;
                    } catch (Throwable t) {
                        Throwable cause = t.getCause();
                        if (cause instanceof InterruptedException || cause instanceof InterruptedIOException || cause instanceof ClosedByInterruptException) {
                            // ignore
                        } else {
                            LOG.warn("Parallel gather failed", t);
                        }
                        return null;
                    }
                }));
            }
        }

        Map<MiruTermId, TermIdLastIdCount> results = Maps.newHashMap();
        boolean done = false;
        for (Future<List<TermIdLastIdCount>> future : futures) {
            if (future != null) {
                if (done) {
                    future.cancel(true);
                } else {
                    List<TermIdLastIdCount> got = future.get();
                    if (got != null) {
                        for (TermIdLastIdCount termIdLastIdCount : got) {
                            TermIdLastIdCount existing = results.putIfAbsent(termIdLastIdCount.termId, termIdLastIdCount);
                            if (existing != null) {
                                existing.lastId = Math.max(existing.lastId, termIdLastIdCount.lastId);
                                existing.count += termIdLastIdCount.count;
                            } else if (limit >= 0 && results.size() > limit) {
                                done = true;
                            }
                        }
                    }
                }
            }
        }

        int count = 0;
        for (TermIdLastIdCount termIdLastIdCount : results.values()) {
            if ((limit >= 0 && count > limit) || !stream.stream(termIdLastIdCount)) {
                break;
            }
            count++;
        }
    }

    public <BM extends IBM, IBM, S extends MiruSipCursor<S>> void gather(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, S> requestContext,
        BM answer,
        int pivotFieldId,
        int batchSize,
        boolean descending,
        boolean includeCounts,
        Optional<BM> counter,
        MiruSolutionLog solutionLog,
        LastIdAndTermIdStream lastIdAndTermIdStream,
        StackBuffer stackBuffer) throws Exception {

        gatherActivityLookup(name,
            bitmaps,
            requestContext,
            answer,
            pivotFieldId,
            batchSize,
            descending,
            includeCounts,
            counter,
            solutionLog,
            lastIdAndTermIdStream,
            stackBuffer);

        /*MiruFieldDefinition fieldDefinition = requestContext.getSchema().getFieldDefinition(pivotFieldId);
        if (fieldDefinition.type.hasFeature(MiruFieldDefinition.Feature.indexedValueBits)) {
            gatherValueBits(name, bitmaps, requestContext, answer, pivotFieldId, solutionLog, termIdStream, stackBuffer);
        } else {
            gatherActivityLookup(name, bitmaps, requestContext, answer, pivotFieldId, batchSize, solutionLog, termIdStream, stackBuffer);
        }*/
    }

    // ooh that tickles
    /*private <BM extends IBM, IBM, S extends MiruSipCursor<S>> void gatherValueBits(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, S> requestContext,
        BM answer,
        int pivotFieldId,
        MiruSolutionLog solutionLog,
        TermIdStream termIdStream,
        StackBuffer stackBuffer) throws Exception {

        int[][] featureFieldIds = new int[][] { new int[] { pivotFieldId } };
        gatherFeatures(name,
            bitmaps,
            requestContext,
            streamBitmaps -> streamBitmaps.stream(0, -1, null, -1, answer),
            featureFieldIds,
            true,
            (streamIndex, lastId, answerTermId, answerScoredLastId, featureId, termIds) -> featureId == -1 || termIdStream.stream(termIds[0]),
            solutionLog,
            stackBuffer);
    }*/

    private <BM extends IBM, IBM, S extends MiruSipCursor<S>> void gatherActivityLookup(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, S> requestContext,
        BM answer,
        int pivotFieldId,
        int batchSize,
        boolean descending,
        boolean includeCounts,
        Optional<BM> counter,
        MiruSolutionLog solutionLog,
        LastIdAndTermIdStream lastIdAndTermIdStream,
        StackBuffer stackBuffer) throws Exception {

        MiruActivityIndex activityIndex = requestContext.getActivityIndex();
        MiruSchema schema = requestContext.getSchema();
        MiruFieldIndex<BM, IBM> primaryFieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);

        if (bitmaps.supportsInPlace()) {
            // don't mutate the original
            answer = bitmaps.copy(answer);
        }

        FieldMultiTermTxIndex<BM, IBM> multiTermTxIndex = new FieldMultiTermTxIndex<>(name, primaryFieldIndex, pivotFieldId, -1);
        TObjectIntHashMap<MiruTermId> distincts = new TObjectIntHashMap<>(batchSize);

        int[] ids = new int[batchSize];
        int gets = 0;
        int fetched = 0;
        long getAllCost = 0;
        long andNotCost = 0;
        done:
        while (!bitmaps.isEmpty(answer)) {
            MiruIntIterator intIterator = descending ? bitmaps.descendingIntIterator(answer) : bitmaps.intIterator(answer);
            int added = 0;
            Arrays.fill(ids, -1);
            while (intIterator.hasNext() && added < batchSize) {
                ids[added] = intIterator.next();
                added++;
            }
            fetched += added;

            int[] actualIds = new int[added];
            System.arraycopy(ids, 0, actualIds, 0, added);

            gets++;

            long start = System.nanoTime();
            MiruTermId[][] all = activityIndex.getAll(name, actualIds, schema.getFieldDefinition(pivotFieldId), stackBuffer);
            getAllCost += (System.nanoTime() - start);
            distincts.clear();

            for (int i = 0; i < all.length; i++) {
                MiruTermId[] termIds = all[i];
                if (termIds != null && termIds.length > 0) {
                    for (MiruTermId termId : termIds) {
                        distincts.putIfAbsent(termId, ids[i]);
                    }
                }
            }

            MiruTermId[] termIds = new MiruTermId[distincts.size()];
            int[] lastIds = new int[distincts.size()];
            TObjectIntIterator<MiruTermId> iter = distincts.iterator();
            for (int i = 0; iter.hasNext(); i++) {
                iter.advance();
                termIds[i] = iter.key();
                lastIds[i] = iter.value();
            }

            long[] counts = includeCounts ? new long[distincts.size()] : null;

            start = System.nanoTime();
            MiruTermId[] consumableTermIds = new MiruTermId[termIds.length];
            System.arraycopy(termIds, 0, consumableTermIds, 0, termIds.length);
            multiTermTxIndex.setTermIds(consumableTermIds);
            if (bitmaps.supportsInPlace()) {
                bitmaps.inPlaceAndNotMultiTx(answer, multiTermTxIndex, counts, counter, stackBuffer);
            } else {
                answer = bitmaps.andNotMultiTx(answer, multiTermTxIndex, counts, counter, stackBuffer);
            }
            if (bitmaps.supportsInPlace()) {
                bitmaps.inPlaceAndNot(answer, bitmaps.createWithBits(actualIds));
                //TODO possibly buggy, need to reevaluate
                /*bitmaps.inPlaceRemoveRange(answer, actualIds[0], actualIds[actualIds.length - 1] + 1);*/
            } else {
                answer = bitmaps.andNot(answer, bitmaps.createWithBits(actualIds));
                //TODO possibly buggy, need to reevaluate
                /*answer = bitmaps.removeRange(answer, actualIds[0], actualIds[actualIds.length - 1] + 1);*/
            }
            andNotCost += (System.nanoTime() - start);

            for (int i = 0; i < termIds.length; i++) {
                if (!lastIdAndTermIdStream.stream(lastIds[i], termIds[i], includeCounts ? counts[i] : -1)) {
                    break done;
                }
            }
        }
        solutionLog.log(MiruSolutionLogLevel.INFO, "gather aggregate gets:{} fetched:{} getAllCost:{} andNotCost:{}",
            gets, fetched, getAllCost, andNotCost);
    }

    public <BM extends IBM, IBM> BM filter(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> context,
        MiruFilter filter,
        MiruSolutionLog solutionLog,
        Map<FieldAndTermId, MutableInt> termCollector,
        int largestIndex,
        int considerIfIndexIdGreaterThanN,
        int maxWildcardExpansion,
        StackBuffer stackBuffer)
        throws Exception {
        return filterInOut(name,
            bitmaps,
            context,
            filter,
            solutionLog,
            termCollector,
            true,
            largestIndex,
            considerIfIndexIdGreaterThanN,
            maxWildcardExpansion,
            stackBuffer);
    }

    private <BM extends IBM, IBM> BM filterInOut(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> context,
        MiruFilter filter,
        MiruSolutionLog solutionLog,
        Map<FieldAndTermId, MutableInt> termCollector,
        boolean termIn,
        int largestIndex,
        int considerIfLastIdGreaterThanN,
        int maxWildcardExpansion,
        StackBuffer stackBuffer)
        throws Exception {

        MiruSchema schema = context.getSchema();
        MiruTermComposer termComposer = context.getTermComposer();
        MiruFieldIndexProvider<BM, IBM> fieldIndexProvider = context.getFieldIndexProvider();
        List<MiruTxIndex<IBM>> filterBitmaps = new ArrayList<>();
        if (filter.inclusiveFilter) {
            filterBitmaps.add(new SimpleInvertedIndex<>(bitmaps.buildIndexMask(largestIndex, context.getRemovalIndex(), null, stackBuffer)));
        }
        if (filter.fieldFilters != null) {
            boolean abortIfEmpty = filter.operation == MiruFilterOperation.and;
            for (MiruFieldFilter fieldFilter : filter.fieldFilters) {
                int fieldId = schema.getFieldId(fieldFilter.fieldName);
                if (fieldId >= 0) {
                    MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);
                    final List<MiruTermId> fieldTermIds = new ArrayList<>();
                    boolean fieldTermIn = filter.operation == MiruFilterOperation.pButNotQ && !filterBitmaps.isEmpty() ? !termIn : termIn;
                    long start = System.currentTimeMillis();
                    List<MiruValue> values = fieldFilter.values != null ? fieldFilter.values : Collections.emptyList();
                    MiruFieldIndex<BM, IBM> fieldIndex = fieldIndexProvider.getFieldIndex(fieldFilter.fieldType);
                    for (MiruValue value : values) {
                        if (fieldDefinition.prefix.type != MiruFieldDefinition.Prefix.Type.none && value.last().equals("*")) {
                            String[] baseParts = value.slice(0, value.parts.length - 1);
                            byte[] lowerInclusive = termComposer.prefixLowerInclusive(schema, fieldDefinition, stackBuffer, baseParts);
                            byte[] upperExclusive = termComposer.prefixUpperExclusive(schema, fieldDefinition, stackBuffer, baseParts);
                            int[] count = { 0 };
                            fieldIndex.streamTermIdsForField(name, fieldId,
                                Collections.singletonList(new KeyRange(lowerInclusive, upperExclusive)),
                                termId -> {
                                    if (termId != null) {
                                        collectTerm(fieldId, termId, fieldTermIn, fieldTermIds, termCollector);
                                        count[0]++;
                                    }
                                    return maxWildcardExpansion <= 0 || count[0] < maxWildcardExpansion;
                                }, stackBuffer);
                        } else {
                            MiruTermId termId = termComposer.compose(schema, fieldDefinition, stackBuffer, value.parts);
                            collectTerm(fieldId, termId, fieldTermIn, fieldTermIds, termCollector);
                        }
                    }
                    solutionLog.log(MiruSolutionLogLevel.DEBUG, "filter: fieldId={} values={} lookup took {} millis.",
                        fieldId, values.size(), System.currentTimeMillis() - start);
                    if (abortIfEmpty && fieldTermIds.isEmpty()) {
                        // implicitly empty results, "and" operation would also be empty
                        return bitmaps.create();
                    } else if (!fieldTermIds.isEmpty()) {
                        start = System.currentTimeMillis();
                        MiruTermId[] termIds = fieldTermIds.toArray(new MiruTermId[fieldTermIds.size()]);
                        FieldMultiTermTxIndex<BM, IBM> multiTxIndex = new FieldMultiTermTxIndex<>(name, fieldIndex, fieldId, considerIfLastIdGreaterThanN);
                        multiTxIndex.setTermIds(termIds);
                        BM r = bitmaps.orMultiTx(multiTxIndex, stackBuffer);
                        filterBitmaps.add(new SimpleInvertedIndex<>(r));
                        solutionLog.log(MiruSolutionLogLevel.DEBUG, "filter: fieldId={} bitmaps={} aggregate took {} millis.",
                            fieldId, fieldTermIds.size(), System.currentTimeMillis() - start);
                    }
                } else {
                    solutionLog.log(MiruSolutionLogLevel.INFO, "schema lacks field definition for fieldName:" + fieldFilter.fieldName);
                }
            }
        }
        if (filter.subFilters != null) {
            for (MiruFilter subFilter : filter.subFilters) {
                boolean subTermIn = (filter.operation == MiruFilterOperation.pButNotQ && !filterBitmaps.isEmpty()) ? !termIn : termIn;
                BM subStorage = filterInOut(name, bitmaps, context, subFilter, solutionLog,
                    termCollector, subTermIn, largestIndex, considerIfLastIdGreaterThanN, maxWildcardExpansion, stackBuffer);
                filterBitmaps.add(new SimpleInvertedIndex<>(subStorage));
            }
        }
        return executeFilter(bitmaps, filter.operation, solutionLog, filterBitmaps, stackBuffer);
    }

    private void collectTerm(int fieldId,
        MiruTermId termId,
        boolean fieldTermIn,
        List<MiruTermId> fieldTermIds,
        Map<FieldAndTermId, MutableInt> termCollector) throws Exception {

        fieldTermIds.add(termId);

        if (termCollector != null) {
            MutableInt count = termCollector.computeIfAbsent(new FieldAndTermId(fieldId, termId), key -> new MutableInt());
            if (fieldTermIn) {
                count.increment();
            } else {
                count.decrement();
            }
        }
    }

    private <BM extends IBM, IBM> BM executeFilter(MiruBitmaps<BM, IBM> bitmaps,
        MiruFilterOperation operation,
        MiruSolutionLog solutionLog,
        List<MiruTxIndex<IBM>> filterBitmaps,
        StackBuffer stackBuffer) throws Exception {

        BM bitmapStorage;
        long start = System.currentTimeMillis();
        if (operation == MiruFilterOperation.and) {
            bitmapStorage = bitmaps.andTx(filterBitmaps, stackBuffer);
        } else if (operation == MiruFilterOperation.or) {
            bitmapStorage = bitmaps.orTx(filterBitmaps, stackBuffer);
        } else if (operation == MiruFilterOperation.pButNotQ) {
            bitmapStorage = bitmaps.andNotTx(filterBitmaps.get(0), filterBitmaps.subList(1, filterBitmaps.size()), stackBuffer);
        } else {
            throw new UnsupportedOperationException(operation + " isn't currently supported.");
        }
        solutionLog.log(MiruSolutionLogLevel.DEBUG, "executeFilter: aggregate took {} millis.", System.currentTimeMillis() - start);
        return bitmapStorage;
    }

    @Override
    public String toString() {
        return "MiruAggregateUtil{" + '}';
    }

}
