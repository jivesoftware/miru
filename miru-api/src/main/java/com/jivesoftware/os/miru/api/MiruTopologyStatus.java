package com.jivesoftware.os.miru.api;

/**
 *
 */
public class MiruTopologyStatus {

    public final MiruPartition partition;
    public final long lastActiveTimestamp;

    public MiruTopologyStatus(MiruPartition partition, long lastActiveTimestamp) {
        this.partition = partition;
        this.lastActiveTimestamp = lastActiveTimestamp;
    }

    @Override
    public String toString() {
        return "MiruTopologyStatus{" +
            "partition=" + partition +
            ", lastActiveTimestamp=" + lastActiveTimestamp +
            '}';
    }
}
