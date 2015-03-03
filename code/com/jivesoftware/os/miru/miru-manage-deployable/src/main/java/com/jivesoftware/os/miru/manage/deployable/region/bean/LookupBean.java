package com.jivesoftware.os.miru.manage.deployable.region.bean;

import com.jivesoftware.os.miru.wal.lookup.MiruActivityLookupEntry;

/**
 *
 */
public class LookupBean {

    private final String activityTimestamp;
    private final int partitionId;
    private final int writerId;
    private final int index;
    private final boolean removed;
    private final String version;

    public LookupBean(long activityTimestamp, MiruActivityLookupEntry entry, long version) {
        this.activityTimestamp = String.valueOf(activityTimestamp);
        this.partitionId = entry.partitionId;
        this.writerId = entry.writerId;
        this.index = entry.index;
        this.removed = entry.removed;
        this.version = String.valueOf(version);
    }

    public String getActivityTimestamp() {
        return activityTimestamp;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getWriterId() {
        return writerId;
    }

    public int getIndex() {
        return index;
    }

    public boolean isRemoved() {
        return removed;
    }

    public String getVersion() {
        return version;
    }
}
