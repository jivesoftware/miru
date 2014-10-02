package com.jivesoftware.os.miru.service.index.memory;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * In-memory impl. Activity data lives in an array, last index is a simple integer.
 */
public class MiruInMemoryActivityIndex implements MiruActivityIndex, BulkImport<Iterator<MiruInternalActivity>>, BulkExport<Iterator<MiruInternalActivity>> {

    private final int initialCapacity = 32; //TODO configure?
    private MiruInternalActivity[] activities;
    private final Object activityLock = new Object();
    private int last = -1;
    private long activitySizeInBytes = 0;

    public MiruInMemoryActivityIndex() {
        this.activities = new MiruInternalActivity[initialCapacity];
    }

    @Override
    public MiruInternalActivity get(MiruTenantId tenantId, int index) {
        MiruInternalActivity[] activities = this.activities; // stable reference
        int capacity = activities.length;
        checkArgument(index >= 0 && index < capacity, "Index parameter is out of bounds. The value %s must be >=0 and <%s", index, capacity);
        return index < activities.length ? activities[index] : null;
    }

    @Override
    public MiruTermId[] get(MiruTenantId tenantId, int index, int fieldId) {
        MiruInternalActivity activity = get(tenantId, index);
        return activity.fieldsValues[fieldId];
    }

    @Override
    public int lastId() {
        return last;
    }

    @Override
    public void setAndReady(List<MiruActivityAndId<MiruInternalActivity>> activityAndIds) throws Exception {
        if (!activityAndIds.isEmpty()) {
            set(activityAndIds);
            ready(activityAndIds.get(activityAndIds.size() - 1).id);
        }
    }

    @Override
    public void set(List<MiruActivityAndId<MiruInternalActivity>> activityAndIds) {
        for (MiruActivityAndId<MiruInternalActivity> activityAndId : activityAndIds) {
            int index = activityAndId.id;
            MiruInternalActivity activity = activityAndId.activity;
            synchronized (activityLock) {
                checkArgument(index >= 0, "Index parameter is out of bounds. The value %s must be >=0", index);
                if (index >= activities.length) {
                    int newLength = activities.length * 2;
                    while (newLength <= index) {
                        newLength *= 2;
                    }
                    MiruInternalActivity[] newActivities = new MiruInternalActivity[newLength];
                    System.arraycopy(activities, 0, newActivities, 0, activities.length);
                    activities = newActivities;
                }
                if (activities[index] != null) {
                    activitySizeInBytes -= activities[index].sizeInBytes();
                }
                activities[index] = activity;
                activitySizeInBytes += activity.sizeInBytes();
            }
        }
    }

    @Override
    public void ready(int index) throws Exception {
        last = Math.max(index, last);
    }

    @Override
    public long sizeInMemory() {
        return activities.length * 8 + activitySizeInBytes;
    }

    @Override
    public long sizeOnDisk() throws Exception {
        return 0;
    }

    @Override
    public void close() {
    }

    @Override
    public void bulkImport(MiruTenantId tenantId, BulkExport<Iterator<MiruInternalActivity>> importItems) throws Exception {
        Iterator<MiruInternalActivity> importActivities = importItems.bulkExport(tenantId);
        int batchSize = 1_000; //TODO expose to config

        synchronized (activityLock) {
            List<MiruActivityAndId<MiruInternalActivity>> batch = new ArrayList<>(batchSize);
            int index = 0;
            while (importActivities.hasNext()) {
                MiruInternalActivity next = importActivities.next();
                if (next == null) {
                    break;
                }

                batch.add(new MiruActivityAndId<>(next, index));
                index++;
                if (batch.size() >= batchSize) {
                    setAndReady(batch);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                setAndReady(batch);
            }
            MiruInternalActivity[] compact = new MiruInternalActivity[last + 1];
            System.arraycopy(activities, 0, compact, 0, compact.length);
            activities = compact;
        }

    }

    @Override
    public Iterator<MiruInternalActivity> bulkExport(final MiruTenantId tenantId) throws Exception {
        MiruInternalActivity[] activities = this.activities; // stable reference
        return Arrays.asList(activities).iterator();
    }
}
