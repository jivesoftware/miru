package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruIndexUtil;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 */
public class MiruIndexLatest<BM> {

    private final MiruTermId fieldAggregateTermId = new MiruIndexUtil().makeLatestTerm();

    public List<Future<?>> index(final MiruContext<BM, ?> context,
        List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds,
        final boolean repair,
        ExecutorService indexExecutor)
        throws Exception {

        final MiruFieldIndex<BM> allFieldIndex = context.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);
        final MiruFieldIndex<BM> latestFieldIndex = context.fieldIndexProvider.getFieldIndex(MiruFieldType.latest);
        List<MiruFieldDefinition> writeTimeAggregateFields = context.schema.getFieldsWithLatest();
        // rough estimate of necessary capacity
        List<Future<?>> futures = Lists.newArrayListWithCapacity(internalActivityAndIds.size() * writeTimeAggregateFields.size());
        for (final MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
            for (final MiruFieldDefinition fieldDefinition : writeTimeAggregateFields) {
                final MiruTermId[] fieldValues = internalActivityAndId.activity.fieldsValues[fieldDefinition.fieldId];
                if (fieldValues != null && fieldValues.length > 0) {
                    futures.add(indexExecutor.submit(() -> {
                        byte[] primitiveBuffer = new byte[8];
                        // Answers the question,
                        // "What is the latest activity against each distinct value of this field?"
                        MiruInvertedIndex<BM> aggregateIndex = latestFieldIndex.getOrCreateInvertedIndex(
                            fieldDefinition.fieldId, fieldAggregateTermId);

                        // ["doc"] -> "d1", "d2", "d3", "d4" -> [0, 1(d1), 0, 0, 1(d2), 0, 0, 1(d3), 0, 0, 1(d4)]
                        for (MiruTermId fieldValue : fieldValues) {
                            MiruInvertedIndex<BM> fieldValueIndex = allFieldIndex.get(fieldDefinition.fieldId, fieldValue);
                            Optional<BM> optionalIndex = fieldValueIndex.getIndexUnsafe(primitiveBuffer);
                            if (optionalIndex.isPresent()) {
                                aggregateIndex.andNotToSourceSize(Collections.singletonList(optionalIndex.get()), primitiveBuffer);
                            }
                        }
                        if (repair) {
                            latestFieldIndex.set(fieldDefinition.fieldId, fieldAggregateTermId, new int[]{internalActivityAndId.id}, null, primitiveBuffer);
                        } else {
                            latestFieldIndex.append(fieldDefinition.fieldId, fieldAggregateTermId, new int[]{internalActivityAndId.id}, null, primitiveBuffer);
                        }
                        return null;
                    }));
                }
            }
        }
        return futures;
    }

}
