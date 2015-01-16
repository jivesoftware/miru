package com.jivesoftware.os.miru.cluster;

/**
 *
 */
public class MiruPartitionActiveTimestamp {

    public final boolean active;
    public final long timestamp;

    public MiruPartitionActiveTimestamp(boolean active, long timestamp) {
        this.active = active;
        this.timestamp = timestamp;
    }
}
