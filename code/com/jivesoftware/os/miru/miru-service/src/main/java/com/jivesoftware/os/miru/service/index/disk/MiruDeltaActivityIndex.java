package com.jivesoftware.os.miru.service.index.disk;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * DELTA FORCE
 */
public class MiruDeltaActivityIndex implements MiruActivityIndex {

    private final MiruActivityIndex backingIndex;
    private final List<MiruActivityAndId<MiruInternalActivity>> activities = Lists.newArrayList();
    private final AtomicInteger offset = new AtomicInteger(-1);
    private final AtomicInteger lastId = new AtomicInteger(-1);

    public MiruDeltaActivityIndex(MiruActivityIndex backingIndex) {
        this.backingIndex = backingIndex;
    }

    @Override
    public MiruInternalActivity get(MiruTenantId tenantId, int index) {
        int base = offset.get();
        if (base < 0 || index < base) {
            return backingIndex.get(tenantId, index);
        } else {
            int pos = index - base;
            checkArgument(pos >= 0 && index < activities.size(), "Index parameter is out of bounds. The value %s must be >=0 and <%s", 0, activities.size());
            return activities.get(pos).activity;
        }
    }

    @Override
    public MiruTermId[] get(MiruTenantId tenantId, int index, int fieldId) {
        int base = offset.get();
        if (base < 0 || index < base) {
            return backingIndex.get(tenantId, index, fieldId);
        } else {
            int pos = index - base;
            if (pos < activities.size()) {
                return activities.get(pos).activity.fieldsValues[fieldId];
            } else {
                return null;
            }
        }
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
    public void setAndReady(List<MiruActivityAndId<MiruInternalActivity>> activityAndIds) throws Exception {
        set(activityAndIds);
        ready(activityAndIds.get(activityAndIds.size() - 1).id);
    }

    @Override
    public void set(List<MiruActivityAndId<MiruInternalActivity>> activityAndIds) {
        if (activityAndIds.isEmpty()) {
            return;
        }
        int base = offset.get();
        if (base < 0) {
            int index = activityAndIds.get(0).id;
            checkArgument(index >= 0, "Index parameter is out of bounds. The value %s must be >=0", index);
            offset.set(index);
        }
        activities.addAll(activityAndIds);
    }

    @Override
    public void ready(int index) throws Exception {
        lastId.set(index);
    }

    @Override
    public void close() {
        backingIndex.close();
    }

    public void merge() throws Exception {
        backingIndex.setAndReady(activities);
        activities.clear();
    }
}
