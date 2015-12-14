package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruIndexUtil;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 */
public class MiruIndexFirst<BM extends IBM, IBM> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruTermId fieldAggregateTermId = new MiruIndexUtil().makeLatestTerm();

    public List<Future<?>> index(final MiruContext<BM, IBM, ?> context,
        MiruTenantId tenantId, List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds,
        final boolean repair,
        ExecutorService indexExecutor)
        throws Exception {

        final MiruFieldIndex<BM, IBM> primaryFieldIndex = context.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);
        List<MiruFieldDefinition> referenceFields = context.schema.getFieldsWithFirst();
        // rough estimate of necessary capacity
        List<Future<?>> futures = Lists.newArrayListWithCapacity(internalActivityAndIds.size() * referenceFields.size());
        for (final MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
            for (final MiruFieldDefinition fieldDefinition : referenceFields) {
                final MiruTermId[] fieldValues = internalActivityAndId.activity.fieldsValues[fieldDefinition.fieldId];
                if (fieldValues != null && fieldValues.length > 0) {
                    futures.add(indexExecutor.submit(() -> {
                        StackBuffer stackBuffer = new StackBuffer();
                        // Simply ensures the term appears in the index. Bitmap is not accumulated.
                        MiruInvertedIndex<BM, IBM> aggregateIndex = primaryFieldIndex.getOrCreateInvertedIndex(
                            fieldDefinition.fieldId, fieldAggregateTermId);
                        aggregateIndex.setIfEmpty(stackBuffer, internalActivityAndId.id);
                        return null;
                    }));
                }
            }
        }
        return futures;
    }

}
