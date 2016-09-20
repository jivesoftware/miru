package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
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
public class MiruIndexLatest<BM extends IBM, IBM> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruTermId fieldAggregateTermId = new MiruIndexUtil().makeLatestTerm();

    public List<Future<?>> index(final MiruContext<BM, IBM, ?> context,
        MiruTenantId tenantId, List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds,
        ExecutorService indexExecutor)
        throws Exception {

        final MiruFieldIndex<BM, IBM> allFieldIndex = context.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);
        final MiruFieldIndex<BM, IBM> latestFieldIndex = context.fieldIndexProvider.getFieldIndex(MiruFieldType.latest);
        List<MiruFieldDefinition> writeTimeAggregateFields = context.schema.getFieldsWithLatest();
        // rough estimate of necessary capacity
        List<Future<?>> futures = Lists.newArrayListWithCapacity(internalActivityAndIds.size() * writeTimeAggregateFields.size());
        for (final MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
            for (final MiruFieldDefinition fieldDefinition : writeTimeAggregateFields) {
                final MiruTermId[] fieldValues = internalActivityAndId.activity.fieldsValues[fieldDefinition.fieldId];
                if (fieldValues != null && fieldValues.length > 0) {
                    futures.add(indexExecutor.submit(() -> {
                        StackBuffer stackBuffer = new StackBuffer();
                        // Answers the question,
                        // "What is the latest activity against each distinct value of this field?"
                        MiruInvertedIndex<BM, IBM> aggregateIndex = latestFieldIndex.getOrCreateInvertedIndex("indexLatest",
                            fieldDefinition.fieldId, fieldAggregateTermId);

                        // ["doc"] -> "d1", "d2", "d3", "d4" -> [0, 1(d1), 0, 0, 1(d2), 0, 0, 1(d3), 0, 0, 1(d4)]
                        BitmapAndLastId<BM> container = new BitmapAndLastId<>();
                        for (MiruTermId fieldValue : fieldValues) {
                            MiruInvertedIndex<BM, IBM> fieldValueIndex = allFieldIndex.get("indexLatest", fieldDefinition.fieldId, fieldValue);
                            container.clear();
                            fieldValueIndex.getIndex(container, stackBuffer);
                            if (container.isSet()) {
                                log.inc("count>andNot", 1);
                                log.inc("count>andNot", 1, tenantId.toString());
                                aggregateIndex.andNotToSourceSize(Collections.singletonList(container.getBitmap()), stackBuffer);
                            }
                        }

                        log.inc("count>set", 1);
                        log.inc("count>set", 1, tenantId.toString());
                        latestFieldIndex.set(fieldDefinition.fieldId, fieldAggregateTermId, new int[] { internalActivityAndId.id }, null, stackBuffer);

                        return null;
                    }));
                }
            }
        }
        return futures;
    }

}
