package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition.Feature;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.plugin.index.ValueBitsIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 *
 */
public class MiruIndexValueBits<BM extends IBM, IBM> {

    private final static MetricLogger log = MetricLoggerFactory.getLogger();

    public List<Future<List<ValueIndexWork>>> compose(MiruContext<BM, IBM, ?> context,
        final List<MiruActivityAndId<MiruInternalActivity>> internalActivityAndIds,
        ExecutorService indexExecutor)
        throws Exception {

        TIntList activityIds = null;
        MiruFieldDefinition[] fieldDefinitions = context.getSchema().getFieldDefinitions();
        List<Future<List<ValueIndexWork>>> workFutures = new ArrayList<>(fieldDefinitions.length);
        for (final MiruFieldDefinition fieldDefinition : fieldDefinitions) {
            if (!fieldDefinition.type.hasFeature(Feature.indexedValueBits)) {
                workFutures.add(Futures.immediateFuture(Collections.emptyList()));
                continue;
            }
            if (activityIds == null) {
                activityIds = new TIntArrayList(internalActivityAndIds.size());
                for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
                    activityIds.add(internalActivityAndId.id);
                }
            }
            TIntList allIds = activityIds;
            workFutures.add(indexExecutor.submit(() -> {
                TIntList[] bitIds = new TIntList[0];
                for (MiruActivityAndId<MiruInternalActivity> internalActivityAndId : internalActivityAndIds) {
                    MiruInternalActivity activity = internalActivityAndId.activity;
                    MiruTermId[] fieldValues = activity.fieldsValues[fieldDefinition.fieldId];
                    if (fieldValues != null && fieldValues.length > 0) {
                        byte[] rawValue = fieldValues[0].getBytes();
                        byte[] value = ValueBitsIndex.packValue(rawValue);


                        for (int aye = 0; aye < value.length; aye++) {
                            int v = value[aye] & 0xFF;
                            int bit = aye * 8;
                            while (v != 0) {
                                if ((v & 1) != 0) {
                                    if ((bit + 1) > bitIds.length) {
                                        TIntList[] grow = new TIntList[bit + 1];
                                        System.arraycopy(bitIds, 0, grow, 0, bitIds.length);
                                        bitIds = grow;
                                    }
                                    if (bitIds[bit] == null) {
                                        bitIds[bit] = new TIntArrayList();
                                    }
                                    bitIds[bit].add(internalActivityAndId.id);
                                }
                                bit++;
                                v >>>= 1;
                            }
                        }
                    }
                }
                List<ValueIndexWork> workList = Lists.newArrayListWithCapacity(bitIds.length);
                for (int aye = 0; aye < bitIds.length; aye++) {
                    if (bitIds[aye] != null) {
                        workList.add(new ValueIndexWork(allIds, bitIds[aye], aye));
                    }
                }
                return workList;
            }));
        }
        return workFutures;
    }

    public List<Future<?>> index(final MiruContext<BM, IBM, ?> context,
        MiruTenantId tenantId,
        List<Future<List<ValueIndexWork>>> fieldWorkFutures,
        final boolean repair,
        ExecutorService indexExecutor)
        throws Exception {

        List<ValueIndexWork>[] work = awaitFieldWorkFutures(fieldWorkFutures);

        final MiruFieldIndex<BM, IBM> valueBitsIndex = context.getFieldIndexProvider().getFieldIndex(MiruFieldType.valueBits);
        List<Integer> fieldIds = context.getSchema().getFieldIds();
        List<Future<?>> futures = new ArrayList<>(fieldIds.size());
        for (int fieldId = 0; fieldId < work.length; fieldId++) {
            List<ValueIndexWork> fieldWork = work[fieldId];
            final int finalFieldId = fieldId;
            for (final ValueIndexWork valueIndexWork : fieldWork) {
                futures.add(indexExecutor.submit(() -> {
                    StackBuffer stackBuffer = new StackBuffer();
                    byte[] bit = new byte[2];
                    ValueBitsIndex.shortBytes((short) valueIndexWork.bit, bit, 0);
                    TIntList allIds = valueIndexWork.allIds;
                    TIntList setIds = valueIndexWork.setIds;
                    TIntList removeIds = computeDifference(allIds, setIds);
                    log.inc("count>set", setIds.size());
                    log.inc("count>remove", removeIds.size());
                    log.inc("count>set", setIds.size(), tenantId.toString());
                    log.inc("count>remove", removeIds.size(), tenantId.toString());
                    if (!setIds.isEmpty()) {
                        valueBitsIndex.set(finalFieldId,
                            new MiruTermId(bit),
                            setIds.toArray(),
                            null,
                            stackBuffer);
                    }
                    if (!removeIds.isEmpty()) {
                        valueBitsIndex.remove(finalFieldId,
                            new MiruTermId(bit),
                            removeIds.toArray(),
                            stackBuffer);
                    }
                    return null;
                }));
            }
        }
        return futures;
    }

    public static TIntList computeDifference(TIntList allIds, TIntList setIds) {
        TIntList removeIds = new TIntArrayList();
        for (int i = 0, j = 0; i < allIds.size(); i++) {
            int id = allIds.get(i);
            if (j < setIds.size()) {
                while (j < setIds.size() && setIds.get(j) < id) {
                    j++;
                }
                if (j >= setIds.size() || id != setIds.get(j)) {
                    removeIds.add(id);
                }
            } else {
                removeIds.add(id);
            }
        }
        return removeIds;
    }

    private List<ValueIndexWork>[] awaitFieldWorkFutures(List<Future<List<ValueIndexWork>>> workFutures)
        throws InterruptedException, ExecutionException {

        @SuppressWarnings("unchecked")
        List<ValueIndexWork>[] fieldsWork = new List[workFutures.size()];
        for (int i = 0; i < workFutures.size(); i++) {
            fieldsWork[i] = workFutures.get(i).get();
            Collections.sort(fieldsWork[i]);
        }
        return fieldsWork;
    }

}
