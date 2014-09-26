package com.jivesoftware.os.miru.service.index.memory;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import java.util.Arrays;
import java.util.Iterator;

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
    public void set(int index, MiruInternalActivity activity) {
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
            last = Math.max(index, last);
            activitySizeInBytes += activity.sizeInBytes();
        }
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
        synchronized (activityLock) {
            int index = 0;
            while (importActivities.hasNext()) {
                MiruInternalActivity next = importActivities.next();
                if (next == null) {
                    break;
                }
                set(index, next);
                index++;
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
