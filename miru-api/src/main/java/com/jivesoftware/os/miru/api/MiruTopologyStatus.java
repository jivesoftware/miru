package com.jivesoftware.os.miru.api;

/**
 *
 */
public class MiruTopologyStatus {

    public final MiruPartition partition;
    public final long lastIngressTimestamp;
    public final long lastQueryTimestamp;
    public final long destroyAfterTimestamp;

    public MiruTopologyStatus(MiruPartition partition, long lastIngressTimestamp, long lastQueryTimestamp, long destroyAfterTimestamp) {
        this.partition = partition;
        this.lastIngressTimestamp = lastIngressTimestamp;
        this.lastQueryTimestamp = lastQueryTimestamp;
        this.destroyAfterTimestamp = destroyAfterTimestamp;
    }

    @Override
    public String toString() {
        return "MiruTopologyStatus{" +
            "partition=" + partition +
            ", lastIngressTimestamp=" + lastIngressTimestamp +
            ", lastQueryTimestamp=" + lastQueryTimestamp +
            ", destroyAfterTimestamp=" + destroyAfterTimestamp +
            '}';
    }
}
