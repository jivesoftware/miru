package com.jivesoftware.os.miru.api.topology;

import com.jivesoftware.os.miru.api.MiruPartition;

/**
 *
 */
public class MiruTopologyStatus {

    public final MiruPartition partition;
    public final long lastIngressTimestamp;
    public final long lastQueryTimestamp;
    public final long destroyAfterTimestamp;
    public final long cleanupAfterTimestamp;
    public final long lastTimestamp;
    public final long lastTimestampEpoch;

    public MiruTopologyStatus(MiruPartition partition,
        long lastIngressTimestamp,
        long lastQueryTimestamp,
        long destroyAfterTimestamp,
        long cleanupAfterTimestamp,
        long lastTimestamp,
        long lastTimestampEpoch) {
        this.partition = partition;
        this.lastIngressTimestamp = lastIngressTimestamp;
        this.lastQueryTimestamp = lastQueryTimestamp;
        this.destroyAfterTimestamp = destroyAfterTimestamp;
        this.cleanupAfterTimestamp = cleanupAfterTimestamp;
        this.lastTimestamp = lastTimestamp;
        this.lastTimestampEpoch = lastTimestampEpoch;
    }

    @Override
    public String toString() {
        return "MiruTopologyStatus{" +
            "partition=" + partition +
            ", lastIngressTimestamp=" + lastIngressTimestamp +
            ", lastQueryTimestamp=" + lastQueryTimestamp +
            ", destroyAfterTimestamp=" + destroyAfterTimestamp +
            ", cleanupAfterTimestamp=" + cleanupAfterTimestamp +
            ", lastTimestamp=" + lastTimestamp +
            ", lastTimestampEpoch=" + lastTimestampEpoch +
            '}';
    }
}
