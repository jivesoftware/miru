package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 */
public class MiruIndexFieldValues<BM> {

    private final static MetricLogger log = MetricLoggerFactory.getLogger();

    public List<Future<List<FieldValuesWork>>> compose(MiruContext<BM, ?> context,
        final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds,
        ExecutorService indexExecutor)
        throws Exception {

        MiruFieldDefinition[] fieldDefinitions = context.schema.getFieldDefinitions();
        List<Future<List<FieldValuesWork>>> workFutures = new ArrayList<>(fieldDefinitions.length);
        for (final MiruFieldDefinition fieldDefinition : fieldDefinitions) {
            if (fieldDefinition.type == MiruFieldDefinition.Type.nonIndexed) {
                continue;
            }
            workFutures.add(indexExecutor.submit(() -> {
                Map<MiruTermId, TIntList> fieldWork = Maps.newHashMap();
                for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
                    MiruInternalActivity activity = internalActivityAndId.activity;
                    MiruTermId[] fieldValues = activity.fieldsValues[fieldDefinition.fieldId];
                    if (fieldValues != null) {
                        if (fieldValues.length > 1 && fieldDefinition.type != MiruFieldDefinition.Type.multiTerm) {
                            log.warn("Activity {} field {} type {} != {} but was given {} terms", internalActivityAndId.activity.time,
                                fieldDefinition.name, fieldDefinition.type, MiruFieldDefinition.Type.multiTerm, fieldValues.length);
                        }
                        for (MiruTermId term : fieldValues) {
                            TIntList ids = fieldWork.get(term);
                            if (ids == null) {
                                ids = new TIntArrayList();
                                fieldWork.put(term, ids);
                            }
                            ids.add(internalActivityAndId.id);
                        }
                    }
                }
                List<FieldValuesWork> workList = Lists.newArrayListWithCapacity(fieldWork.size());
                for (Map.Entry<MiruTermId, TIntList> entry : fieldWork.entrySet()) {
                    workList.add(new FieldValuesWork(entry.getKey(), entry.getValue()));
                }
                return workList;
            }));
        }
        return workFutures;
    }

    public List<Future<?>> index(final MiruContext<BM, ?> context,
        List<Future<List<FieldValuesWork>>> fieldWorkFutures,
        final boolean repair,
        ExecutorService indexExecutor)
        throws Exception {

        List<FieldValuesWork>[] work = awaitFieldWorkFutures(fieldWorkFutures);

        final MiruFieldIndex<BM> fieldIndex = context.getFieldIndexProvider().getFieldIndex(MiruFieldType.primary);
        List<Integer> fieldIds = context.schema.getFieldIds();
        List<Future<?>> futures = new ArrayList<>(fieldIds.size());
        for (int fieldId = 0; fieldId < work.length; fieldId++) {
            List<FieldValuesWork> fieldWork = work[fieldId];
            final int finalFieldId = fieldId;
            for (final FieldValuesWork fieldValuesWork : fieldWork) {
                futures.add(indexExecutor.submit(() -> {
                    if (repair) {
                        fieldIndex.set(finalFieldId, fieldValuesWork.fieldValue, fieldValuesWork.ids.toArray());
                    } else {
                        fieldIndex.append(finalFieldId, fieldValuesWork.fieldValue, fieldValuesWork.ids.toArray());
                    }
                    return null;
                }));
            }
        }
        return futures;
    }

    private List<FieldValuesWork>[] awaitFieldWorkFutures(List<Future<List<FieldValuesWork>>> fieldWorkFutures)
        throws InterruptedException, ExecutionException {

        @SuppressWarnings("unchecked")
        List<FieldValuesWork>[] fieldsWork = new List[fieldWorkFutures.size()];
        for (int i = 0; i < fieldWorkFutures.size(); i++) {
            fieldsWork[i] = fieldWorkFutures.get(i).get();
            Collections.sort(fieldsWork[i]);
        }
        return fieldsWork;
    }

}
