package com.jivesoftware.os.miru.plugin.solution;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.FieldMultiTermTxIndex;
import com.jivesoftware.os.miru.plugin.index.IndexTx;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndexProvider;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.MiruTxIndex;
import com.jivesoftware.os.miru.plugin.index.TermIdStream;
import com.jivesoftware.os.miru.plugin.index.ValueIndex;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 */
public class MiruAggregateUtil {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public interface StreamBitmaps<BM> {

        boolean stream(MiruTermId termId, int lastId, BM answer) throws Exception;
    }

    public interface ConsumeBitmaps<BM> {

        boolean consume(StreamBitmaps<BM> streamBitmaps) throws Exception;
    }

    public <BM extends IBM, IBM, S extends MiruSipCursor<S>> void gatherFeatures(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, S> requestContext,
        ConsumeBitmaps<BM> consumeAnswers,
        int[][] featureFieldIds,
        boolean dedupe,
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

        MiruSchema schema = requestContext.getSchema();
        MiruActivityIndex activityIndex = requestContext.getActivityIndex();

        MiruTermId[][][] fieldTerms = new MiruTermId[schema.fieldCount()][][];

        @SuppressWarnings("unchecked")
        Set<Feature>[] features = new Set[featureFieldIds.length];
        if (dedupe) {
            for (int i = 0; i < features.length; i++) {
                features[i] = Sets.newHashSet();
            }
        }

        int[] featureCount = {0};
        int[] termCount = {0};
        int batchSize = 1_000;
        int[] ids = new int[batchSize];
        int[] count = {0};
        long start = System.currentTimeMillis();
        consumeAnswers.consume((answerTermId, answerLastId, answerBitmap) -> {
            termCount[0]++;
            for (Set<Feature> featureSet : features) {
                if (featureSet != null) {
                    featureSet.clear();
                }
            }
            MiruIntIterator iter = bitmaps.intIterator(answerBitmap);
            while (iter.hasNext()) {
                ids[count[0]] = iter.next();
                count[0]++;
                if (count[0] == batchSize) {
                    if (!gatherAndStreamFeatures(name, featureFieldIds, stream, stackBuffer, uniqueFieldIds, activityIndex, fieldTerms,
                        ids, ids.length, answerTermId, answerLastId, featureCount, features)) {
                        return false;
                    }
                    count[0] = 0;
                }
            }
            if (count[0] > 0) {
                if (!gatherAndStreamFeatures(name, featureFieldIds, stream, stackBuffer, uniqueFieldIds, activityIndex, fieldTerms,
                    ids, count[0], answerTermId, answerLastId,  featureCount, features)) {
                    return false;
                }
                count[0] = 0;
            }
            return stream.stream(answerTermId, answerLastId, -1, null);
        });
        solutionLog.log(MiruSolutionLogLevel.INFO, "Gathered {} features for {} terms in {} ms",
            featureCount[0], termCount[0], System.currentTimeMillis() - start);
    }

    private boolean gatherAndStreamFeatures(String name,
        int[][] featureFieldIds,
        FeatureStream stream,
        StackBuffer stackBuffer,
        Set<Integer> uniqueFieldIds,
        MiruActivityIndex activityIndex,
        MiruTermId[][][] fieldTerms,
        int[] ids,
        int count,
        MiruTermId answerTermId,
        int answerLastId,
        int[] featureCount,
        Set<Feature>[] features) throws Exception {

        for (int fieldId : uniqueFieldIds) {
            fieldTerms[fieldId] = activityIndex.getAll(name, ids, 0, count, fieldId, stackBuffer);
        }
        for (int index = 0; index < count; index++) {
            NEXT_FEATURE:
            for (int i = 0; i < featureFieldIds.length; i++) {
                int[] fieldIds = featureFieldIds[i];
                //TODO handle multiTerm fields
                MiruTermId[] termIds = new MiruTermId[fieldIds.length];
                for (int j = 0; j < fieldIds.length; j++) {
                    MiruTermId[] fieldTermIds = fieldTerms[fieldIds[j]][index];
                    if (fieldTermIds == null || fieldTermIds.length == 0) {
                        continue NEXT_FEATURE;
                    }
                    termIds[j] = fieldTermIds[0];
                }
                if (features[i] == null || features[i].add(new Feature(termIds))) {
                    featureCount[0]++;
                    if (!stream.stream(answerTermId, answerLastId, i, termIds)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private <BM extends IBM, IBM, S extends MiruSipCursor<S>> void indexValueBitsGatherFeatures(String name,
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

        consumeAnswers.consume((answerTermId, answerLastId, answerBitmap) -> {
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

                        if (!stream.stream(answerTermId, answerLastId, featureId, featureTermIds)) {
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
    }

    public interface FeatureStream {

        boolean stream(MiruTermId answerTermId, int answerLastId, int featureId, MiruTermId[] termIds) throws Exception;
    }

    public static class Feature {

        public final MiruTermId[] termIds;

        private int hash = 0;

        public Feature(MiruTermId[] termIds) {
            this.termIds = termIds;
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
    }

    private static class FieldBits<BM extends IBM, IBM> implements Comparable<FieldBits<BM, IBM>> {

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
    }

    public <BM extends IBM, IBM, S extends MiruSipCursor<S>> void stream(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        MiruRequestContext<BM, IBM, S> requestContext,
        MiruPartitionCoord coord,
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
        int streamFieldId = requestContext.getSchema().getFieldId(streamField);

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

            MiruTermId[][] all = activityIndex.getAll(name, actualIds, pivotFieldId, stackBuffer);
            MiruTermId[][] streamAll;
            if (streamFieldId != pivotFieldId) {
                streamAll = activityIndex.getAll(name, actualIds, streamFieldId, stackBuffer);
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
                (index, bitmap) -> {
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

    public <BM extends IBM, IBM, S extends MiruSipCursor<S>> void gather(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, S> requestContext,
        BM answer,
        int pivotFieldId,
        int batchSize,
        MiruSolutionLog solutionLog,
        TermIdStream termIdStream,
        StackBuffer stackBuffer) throws Exception {

        gatherActivityLookup(name, bitmaps, requestContext, answer, pivotFieldId, batchSize, solutionLog, termIdStream, stackBuffer);

        /*MiruFieldDefinition fieldDefinition = requestContext.getSchema().getFieldDefinition(pivotFieldId);
        if (fieldDefinition.type.hasFeature(MiruFieldDefinition.Feature.indexedValueBits)) {
            gatherValueBits(name, bitmaps, requestContext, answer, pivotFieldId, solutionLog, termIdStream, stackBuffer);
        } else {
            gatherActivityLookup(name, bitmaps, requestContext, answer, pivotFieldId, batchSize, solutionLog, termIdStream, stackBuffer);
        }*/
    }

    // ooh that tickles
    private <BM extends IBM, IBM, S extends MiruSipCursor<S>> void gatherValueBits(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, S> requestContext,
        BM answer,
        int pivotFieldId,
        MiruSolutionLog solutionLog,
        TermIdStream termIdStream,
        StackBuffer stackBuffer) throws Exception {

        int[][] featureFieldIds = new int[][]{new int[]{pivotFieldId}};
        gatherFeatures(name,
            bitmaps,
            requestContext,
            streamBitmaps -> streamBitmaps.stream(null, -1, answer),
            featureFieldIds,
            true,
            (answerTermId, answerLastId, featureId, termIds) -> featureId == -1 || termIdStream.stream(termIds[0]),
            solutionLog,
            stackBuffer);
    }

    private <BM extends IBM, IBM, S extends MiruSipCursor<S>> void gatherActivityLookup(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, S> requestContext,
        BM answer,
        int pivotFieldId,
        int batchSize,
        MiruSolutionLog solutionLog,
        TermIdStream termIdStream,
        StackBuffer stackBuffer) throws Exception {

        MiruActivityIndex activityIndex = requestContext.getActivityIndex();
        MiruFieldIndex<BM, IBM> primaryFieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);

        if (bitmaps.supportsInPlace()) {
            // don't mutate the original
            answer = bitmaps.copy(answer);
        }

        FieldMultiTermTxIndex<BM, IBM> multiTermTxIndex = new FieldMultiTermTxIndex<>(name, primaryFieldIndex, pivotFieldId, -1);
        Set<MiruTermId> distincts = Sets.newHashSetWithExpectedSize(batchSize);

        int[] ids = new int[batchSize];
        int gets = 0;
        int fetched = 0;
        int used = 0;
        long getAllCost = 0;
        long andNotCost = 0;
        done:
        while (!bitmaps.isEmpty(answer)) {
            MiruIntIterator intIterator = bitmaps.intIterator(answer);
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

            if (bitmaps.supportsInPlace()) {
                bitmaps.inPlaceAndNot(answer, bitmaps.createWithBits(actualIds));
                //TODO possibly buggy, need to reevaluate
                /*bitmaps.inPlaceRemoveRange(answer, actualIds[0], actualIds[actualIds.length - 1] + 1);*/
            } else {
                answer = bitmaps.andNot(answer, bitmaps.createWithBits(actualIds));
                //TODO possibly buggy, need to reevaluate
                /*answer = bitmaps.removeRange(answer, actualIds[0], actualIds[actualIds.length - 1] + 1);*/
            }

            long start = System.nanoTime();
            MiruTermId[][] all = activityIndex.getAll(name, actualIds, pivotFieldId, stackBuffer);
            getAllCost += (System.nanoTime() - start);
            distincts.clear();
            for (MiruTermId[] termIds : all) {
                if (termIds != null && termIds.length > 0) {
                    for (MiruTermId termId : termIds) {
                        if (distincts.add(termId)) {
                            if (!termIdStream.stream(termId)) {
                                break done;
                            }
                        }
                    }
                }
            }

            MiruTermId[] termIds = distincts.toArray(new MiruTermId[distincts.size()]);
            multiTermTxIndex.setTermIds(termIds);

            start = System.nanoTime();
            if (bitmaps.supportsInPlace()) {
                bitmaps.inPlaceAndNotMultiTx(answer, multiTermTxIndex, stackBuffer);
            } else {
                answer = bitmaps.andNotMultiTx(answer, multiTermTxIndex, stackBuffer);
            }
            andNotCost += (System.nanoTime() - start);
        }
        solutionLog.log(MiruSolutionLogLevel.INFO, "gather aggregate gets:{} fetched:{} used:{} getAllCost:{} andNotCost:{}",
            gets, fetched, used, getAllCost, andNotCost);
    }

    public <BM extends IBM, IBM> BM filter(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruSchema schema,
        final MiruTermComposer termComposer,
        final MiruFieldIndexProvider<BM, IBM> fieldIndexProvider,
        MiruFilter filter,
        MiruSolutionLog solutionLog,
        Map<FieldAndTermId, MutableInt> termCollector,
        int largestIndex,
        final int considerIfIndexIdGreaterThanN,
        StackBuffer stackBuffer)
        throws Exception {
        return filterInOut(name, bitmaps, schema, termComposer, fieldIndexProvider, filter, solutionLog, termCollector, true,
            largestIndex, considerIfIndexIdGreaterThanN, stackBuffer);
    }

    private <BM extends IBM, IBM> BM filterInOut(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruSchema schema,
        final MiruTermComposer termComposer,
        final MiruFieldIndexProvider<BM, IBM> fieldIndexProvider,
        MiruFilter filter,
        MiruSolutionLog solutionLog,
        Map<FieldAndTermId, MutableInt> termCollector,
        boolean termIn,
        int largestIndex,
        int considerIfLastIdGreaterThanN,
        StackBuffer stackBuffer)
        throws Exception {

        List<MiruTxIndex<IBM>> filterBitmaps = new ArrayList<>();
        if (filter.inclusiveFilter) {
            filterBitmaps.add(new SimpleInvertedIndex<>(bitmaps.buildIndexMask(largestIndex, Optional.<IBM>absent())));
        }
        if (filter.fieldFilters != null) {
            boolean abortIfEmpty = filter.operation == MiruFilterOperation.and;
            for (final MiruFieldFilter fieldFilter : filter.fieldFilters) {
                final int fieldId = schema.getFieldId(fieldFilter.fieldName);
                final MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(fieldId);
                if (fieldId >= 0) {
                    final List<MiruTermId> fieldTermIds = new ArrayList<>();
                    boolean fieldTermIn = filter.operation == MiruFilterOperation.pButNotQ && !filterBitmaps.isEmpty() ? !termIn : termIn;
                    long start = System.currentTimeMillis();
                    List<MiruValue> values = fieldFilter.values != null ? fieldFilter.values : Collections.emptyList();
                    MiruFieldIndex<BM, IBM> fieldIndex = fieldIndexProvider.getFieldIndex(fieldFilter.fieldType);
                    for (final MiruValue value : values) {
                        if (fieldDefinition.prefix.type != MiruFieldDefinition.Prefix.Type.none && value.last().equals("*")) {
                            String[] baseParts = value.slice(0, value.parts.length - 1);
                            byte[] lowerInclusive = termComposer.prefixLowerInclusive(schema, fieldDefinition, stackBuffer, baseParts);
                            byte[] upperExclusive = termComposer.prefixUpperExclusive(schema, fieldDefinition, stackBuffer, baseParts);
                            fieldIndex.streamTermIdsForField(name, fieldId,
                                Collections.singletonList(new KeyRange(lowerInclusive, upperExclusive)),
                                termId -> {
                                    if (termId != null) {
                                        collectTerm(fieldId, termId, fieldTermIn, fieldTermIds, termCollector);
                                    }
                                    return true;
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
                }
            }
        }
        if (filter.subFilters != null) {
            for (MiruFilter subFilter : filter.subFilters) {
                boolean subTermIn = (filter.operation == MiruFilterOperation.pButNotQ && !filterBitmaps.isEmpty()) ? !termIn : termIn;
                BM subStorage = filterInOut(name, bitmaps, schema, termComposer, fieldIndexProvider, subFilter, solutionLog,
                    termCollector, subTermIn, largestIndex, considerIfLastIdGreaterThanN, stackBuffer);
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

    private static class SimpleInvertedIndex<IBM> implements MiruTxIndex<IBM> {

        private final IBM bitmap;

        public SimpleInvertedIndex(IBM bitmap) {
            this.bitmap = bitmap;
        }

        @Override
        public <R> R txIndex(IndexTx<R, IBM> tx, StackBuffer stackBuffer) throws Exception {
            return tx.tx(bitmap, null, -1, null);
        }
    }

}
