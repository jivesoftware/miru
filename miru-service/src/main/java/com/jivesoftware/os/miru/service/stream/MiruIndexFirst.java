package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 */
public class MiruIndexFirst<BM extends IBM, IBM> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    public List<Future<?>> index(final MiruContext<BM, IBM, ?> context,
        MiruTenantId tenantId,
        List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds,
        final boolean repair,
        ExecutorService indexExecutor)
        throws Exception {

        final MiruFieldIndex<BM, IBM> primaryFieldIndex = context.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);
        List<MiruFieldDefinition> referenceFields = context.getSchema().getFieldsWithFirst();
        List<Future<?>> futures = Lists.newArrayListWithCapacity(referenceFields.size());
        for (final MiruFieldDefinition fieldDefinition : referenceFields) {
            futures.add(indexExecutor.submit(() -> {
                for (final MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
                    final MiruTermId[] fieldValues = internalActivityAndId.activity.fieldsValues[fieldDefinition.fieldId];
                    if (fieldValues != null && fieldValues.length > 0) {
                        StackBuffer stackBuffer = new StackBuffer();
                        for (MiruTermId termId : fieldValues) {
                            // SetIfEmpty ensures only the first occurrence of the term appears in the index.
                            MiruInvertedIndex<BM, IBM> aggregateIndex = primaryFieldIndex.getOrCreateInvertedIndex("indexFirst",
                                fieldDefinition.fieldId, termId);
                            aggregateIndex.setIfEmpty(stackBuffer, internalActivityAndId.id);
                        }
                    }
                }
                return null;
            }));
        }
        return futures;
    }

}
