package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.MiruTxIndex;
import com.jivesoftware.os.miru.plugin.index.ValueIndex;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLog;
import com.jivesoftware.os.miru.plugin.solution.MiruSolutionLogLevel;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkAnswer.FeatureScore;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class Catwalk {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public <BM extends IBM, IBM> CatwalkAnswer model(String name,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruRequestContext<BM, IBM, ?> requestContext,
        MiruRequest<CatwalkQuery> request,
        Optional<CatwalkReport> report,
        BM answer,
        MiruSolutionLog solutionLog) throws Exception {

        StackBuffer stackBuffer = new StackBuffer();

        log.debug("Catwalk for answer={} request={}", answer, request);
        //System.out.println("Number of matches: " + bitmaps.cardinality(answer));

        MiruSchema schema = requestContext.getSchema();
        MiruFieldIndex<BM, IBM> primaryIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        MiruFieldIndex<BM, IBM> valueBitsIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.valueBits);
        MiruTermComposer termComposer = requestContext.getTermComposer();

        Set<String> fields = Sets.newHashSet();
        String[][] featureFields = request.query.featureFields;
        int[][] featureFieldIds = new int[featureFields.length][];
        for (int i = 0; i < featureFields.length; i++) {
            String[] featureField = featureFields[i];
            Collections.addAll(fields, featureField);
            featureFieldIds[i] = new int[featureField.length];
            for (int j = 0; j < featureFields[i].length; j++) {
                featureFieldIds[i][j] = requestContext.getSchema().getFieldId(featureField[j]);
            }
        }

        long start = System.currentTimeMillis();
        byte[][] valueBuffers = new byte[requestContext.getSchema().fieldCount()][];
        List<FieldBits<BM, IBM>> fieldBits = Lists.newArrayList();
        for (String field : fields) {
            int fieldId = requestContext.getSchema().getFieldId(field);
            List<MiruTermId> termIds = Lists.newArrayList();
            valueBitsIndex.streamTermIdsForField(name, fieldId, null,
                termId -> {
                    termIds.add(termId);
                    return true;
                },
                stackBuffer);

            @SuppressWarnings("unchecked")
            BitmapAndLastId<BM>[] results = new BitmapAndLastId[termIds.size()];
            valueBitsIndex.multiGet(name, fieldId, termIds.toArray(new MiruTermId[0]), results, stackBuffer);

            BM[] featureBits = bitmaps.createArrayOf(results.length);
            for (int i = 0; i < results.length; i++) {
                BitmapAndLastId<BM> result = results[i];
                if (bitmaps.supportsInPlace()) {
                    bitmaps.inPlaceAnd(result.bitmap, answer);
                    featureBits[i] = result.bitmap;
                } else {
                    featureBits[i] = bitmaps.and(Arrays.asList(result.bitmap, answer));
                }
            }

            int[] bits = new int[termIds.size()];
            int maxBit = -1;
            for (int i = 0; i < termIds.size(); i++) {
                bits[i] = ValueIndex.bytesShort(termIds.get(i).getBytes());
                maxBit = Math.max(maxBit, bits[i]);
            }

            valueBuffers[fieldId] = new byte[(maxBit + 1) / 8];
            for (int i = 0; i < termIds.size(); i++) {
                MiruTermId termId = termIds.get(i);
                int bit = ValueIndex.bytesShort(termId.getBytes());
                fieldBits.add(new FieldBits<>(fieldId, bit, featureBits[i], bitmaps.cardinality(featureBits[i])));
            }
        }

        solutionLog.log(MiruSolutionLogLevel.INFO, "Setup field bits took {} ms", System.currentTimeMillis() - start);
        start = System.currentTimeMillis();

        @SuppressWarnings("unchecked")
        Multiset<Feature>[] featureValueSets = new Multiset[featureFields.length];
        for (int i = 0; i < featureFields.length; i++) {
            featureValueSets[i] = HashMultiset.create();
        }

        byte[][] values = new byte[valueBuffers.length][];
        Collections.sort(fieldBits);
        while (!fieldBits.isEmpty()) {
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
                    featureValueSets[featureId].add(new Feature(featureTermIds));
                }
            }
            Collections.sort(fieldBits);
        }

        solutionLog.log(MiruSolutionLogLevel.INFO, "Gather feature numerators took {} ms", System.currentTimeMillis() - start);
        start = System.currentTimeMillis();

        @SuppressWarnings("unchecked")
        List<FeatureScore>[] featureScoreResults = new List[featureFields.length];

        for (int i = 0; i < featureValueSets.length; i++) {
            Multiset<Feature> valueSet = featureValueSets[i];
            featureScoreResults[i] = Lists.newArrayListWithCapacity(valueSet.size());
            for (Entry<Feature> entry : valueSet.entrySet()) {
                int[] fieldIds = featureFieldIds[i];
                MiruTermId[] termIds = entry.getElement().termIds;
                MiruValue[] featureValues = new MiruValue[fieldIds.length];
                for (int j = 0; j < fieldIds.length; j++) {
                    featureValues[j] = new MiruValue(termComposer.decompose(schema,
                        schema.getFieldDefinition(fieldIds[j]),
                        stackBuffer,
                        termIds[j]));
                }

                List<MiruTxIndex<IBM>> ands = Lists.newArrayList();
                for (int j = 0; j < fieldIds.length; j++) {
                    ands.add(primaryIndex.get(name, fieldIds[j], termIds[j]));
                }
                BM bitmap = bitmaps.andTx(ands, stackBuffer);
                featureScoreResults[i].add(new FeatureScore(featureValues, entry.getCount(), bitmaps.cardinality(bitmap)));
            }
        }

        solutionLog.log(MiruSolutionLogLevel.INFO, "Gather feature denominators took {} ms", System.currentTimeMillis() - start);

        boolean resultsExhausted = request.query.timeRange.smallestTimestamp > requestContext.getTimeIndex().getLargestTimestamp();

        CatwalkAnswer result = new CatwalkAnswer(featureScoreResults, resultsExhausted);
        log.debug("result={}", result);
        return result;
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


    private static class Feature {

        private final MiruTermId[] termIds;

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

            // Probably incorrect - comparing Object[] arrays with Arrays.equals
            return Arrays.equals(termIds, feature.termIds);

        }

        @Override
        public int hashCode() {
            return termIds != null ? Arrays.hashCode(termIds) : 0;
        }
    }

}
