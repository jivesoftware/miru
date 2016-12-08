package com.jivesoftware.os.miru.stream.plugins.fulltext;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndex.CustomMarshaller;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.IntStream;

/**
 *
 */
public class FullTextGatherer {

    private static final byte[] KEY = "fullText".getBytes(StandardCharsets.UTF_8);

    private static final CustomMarshaller<Integer> MARSHALLER = new CustomMarshaller<Integer>() {
        @Override
        public byte[] toBytes(Integer comparable) {
            return FilerIO.intBytes(comparable);
        }

        @Override
        public Integer fromBytes(byte[] bytes) {
            return FilerIO.bytesInt(bytes);
        }
    };

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public <BM extends IBM, IBM> void gather(MiruRequestContext<BM, IBM, ?> requestContext,
        FieldMapping[] fieldMappings,
        FullTextTermProvider termProvider,
        int batchSize) throws Exception {

        StackBuffer stackBuffer = new StackBuffer();
        MiruActivityIndex activityIndex = requestContext.getActivityIndex();

        Integer fullTextId = requestContext.getSipIndex().getCustom(KEY, MARSHALLER);
        int start = (fullTextId == null) ? 0 : fullTextId + 1;
        int end = activityIndex.lastId(stackBuffer);

        for (int fromId = start, toId = fromId + batchSize - 1; fromId <= end; fromId += batchSize) {
            gatherBatch(requestContext, fieldMappings, termProvider, fromId, Math.min(end, toId), stackBuffer);
            requestContext.getSipIndex().setCustom(KEY, toId, MARSHALLER);
        }
    }

    private <BM extends IBM, IBM> void gatherBatch(MiruRequestContext<BM, IBM, ?> requestContext,
        FieldMapping[] fieldMappings,
        FullTextTermProvider termProvider,
        int fromId,
        int toId,
        StackBuffer stackBuffer) throws Exception {

        MiruFieldIndex<BM, IBM> primaryFieldIndex = requestContext.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        MiruActivityIndex activityIndex = requestContext.getActivityIndex();
        MiruTermComposer termComposer = requestContext.getTermComposer();
        MiruSchema schema = requestContext.getSchema();

        Set<MiruValue> values = Sets.newHashSet();

        int[] indexes = IntStream.rangeClosed(fromId, toId).toArray();
        MiruTermId[][][] fieldTerms = new MiruTermId[fieldMappings.length][][];
        for (int i = 0; i < fieldMappings.length; i++) {
            FieldMapping fieldMapping = fieldMappings[i];

            MiruTermId[][] got = activityIndex.getAll("fullText", indexes, fieldMapping.fromFieldDefinition.fieldId, stackBuffer);
            fieldTerms[i] = got;

            Set<MiruTermId> uniques = Sets.newHashSet();
            for (MiruTermId[] termIds : got) {
                Collections.addAll(uniques, termIds);
            }
            for (MiruTermId unique : uniques) {
                String[] parts = termComposer.decompose(schema, fieldMapping.fromFieldDefinition, stackBuffer, unique);
                values.add(new MiruValue(parts));
            }
        }

        Map<MiruValue, String[]> valueText = termProvider.getText(values);

        for (int i = 0; i < fieldMappings.length; i++) {
            Map<MiruTermId, TIntList> indexWork = Maps.newHashMap();

            FieldMapping fieldMapping = fieldMappings[i];
            MiruTermId[][] got = fieldTerms[i];

            for (int j = 0; j < indexes.length; j++) {
                int index = indexes[j];
                MiruTermId[] terms = got[j];
                for (MiruTermId term : terms) {
                    String[] parts = termComposer.decompose(schema, fieldMapping.fromFieldDefinition, stackBuffer, term);
                    MiruValue value = new MiruValue(parts);
                    String[] text = valueText.get(value);
                    MiruTermId[] textTerms = new MiruTermId[text.length];
                    for (int k = 0; k < text.length; k++) {
                        textTerms[i] = termComposer.compose(schema, fieldMapping.toFieldDefinition, stackBuffer, text[k]);
                        indexWork.computeIfAbsent(textTerms[i], key -> new TIntArrayList()).add(index);
                    }
                }
            }

            for (Entry<MiruTermId, TIntList> entry : indexWork.entrySet()) {
                MiruTermId term = entry.getKey();
                TIntList ids = entry.getValue();
                MiruInvertedIndex<BM, IBM> toIndex = primaryFieldIndex.get("fullText", fieldMapping.toFieldDefinition.fieldId, term);
                toIndex.set(stackBuffer, ids.toArray());
            }
        }
    }

    private static class FieldMapping {
        private final MiruFieldDefinition fromFieldDefinition;
        private final MiruFieldDefinition toFieldDefinition;

        public FieldMapping(MiruFieldDefinition fromFieldDefinition, MiruFieldDefinition toFieldDefinition) {
            this.fromFieldDefinition = fromFieldDefinition;
            this.toFieldDefinition = toFieldDefinition;
        }
    }
}
