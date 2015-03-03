package com.jivesoftware.os.miru.service.index.delta;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.service.index.Mergeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * DELTA FORCE
 */
public class MiruDeltaActivityIndex implements MiruActivityIndex, Mergeable {

    private final MiruActivityIndex backingIndex;
    private final Map<Integer, MiruActivityAndId<MiruInternalActivity>> activities = Maps.newConcurrentMap();
    private final AtomicInteger lastId = new AtomicInteger(-1);

    public MiruDeltaActivityIndex(MiruActivityIndex backingIndex) {
        this.backingIndex = backingIndex;
    }

    @Override
    public MiruInternalActivity get(MiruTenantId tenantId, int index) {
        //TODO consider writing through to the backing index for old indexes to avoid the double lookup
        MiruActivityAndId<MiruInternalActivity> activityAndId = activities.get(index);
        if (activityAndId != null) {
            return activityAndId.activity;
        } else {
            return backingIndex.get(tenantId, index);
        }
    }

    @Override
    public MiruTermId[] get(MiruTenantId tenantId, int index, int fieldId) {
        MiruActivityAndId<MiruInternalActivity> activityAndId = activities.get(index);
        if (activityAndId != null) {
            return activityAndId.activity.fieldsValues[fieldId];
        } else {
            return backingIndex.get(tenantId, index, fieldId);
        }
    }

    @Override
    public List<MiruTermId[]> getAll(MiruTenantId tenantId, int[] indexes, int fieldId) {
        List<MiruTermId[]> allTermIds = Lists.newArrayList();
        boolean missed = false;
        for (int i = 0; i < indexes.length; i++) {
            if (indexes[i] > -1) {
                MiruActivityAndId<MiruInternalActivity> activityAndId = activities.get(indexes[i]);
                if (activityAndId != null) {
                    indexes[i] = -1;
                    allTermIds.add(activityAndId.activity.fieldsValues[fieldId]);
                } else {
                    missed = true;
                }
            }
        }
        if (missed) {
            allTermIds.addAll(backingIndex.getAll(tenantId, indexes, fieldId));
        }
        return allTermIds;
    }

    @Override
    public int lastId() {
        int id = lastId.get();
        if (id < 0) {
            return backingIndex.lastId();
        } else {
            return id;
        }
    }

    @Override
    public void setAndReady(Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds) throws Exception {
        if (!activityAndIds.isEmpty()) {
            int lastIndex = setInternal(activityAndIds);
            ready(lastIndex);
        }
    }

    @Override
    public void set(Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds) {
        if (!activityAndIds.isEmpty()) {
            setInternal(activityAndIds);
        }
    }

    private int setInternal(Collection<MiruActivityAndId<MiruInternalActivity>> activityAndIds) {
        int lastIndex = -1;
        for (MiruActivityAndId<MiruInternalActivity> activityAndId : activityAndIds) {
            checkArgument(activityAndId.id >= 0, "Index parameter is out of bounds. The value %s must be >=0", activityAndId.id);
            activities.put(activityAndId.id, activityAndId);
            lastIndex = Math.max(lastIndex, activityAndId.id);
        }
        return lastIndex;
    }

    @Override
    public void ready(int index) throws Exception {
        lastId.set(index);
    }

    @Override
    public void close() {
        backingIndex.close();
    }

    @Override
    public void merge() throws Exception {
        backingIndex.setAndReady(activities.values());
        activities.clear();
    }
}
