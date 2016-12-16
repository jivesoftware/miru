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
    public final int lastId;
    public final long lastIdTimestamp;

    public MiruTopologyStatus(MiruPartition partition,
        long lastIngressTimestamp,
        long lastQueryTimestamp,
        long destroyAfterTimestamp,
        long cleanupAfterTimestamp,
        int lastId,
        long lastIdTimestamp) {
        this.partition = partition;
        this.lastIngressTimestamp = lastIngressTimestamp;
        this.lastQueryTimestamp = lastQueryTimestamp;
        this.destroyAfterTimestamp = destroyAfterTimestamp;
        this.cleanupAfterTimestamp = cleanupAfterTimestamp;
        this.lastId = lastId;
        this.lastIdTimestamp = lastIdTimestamp;
    }

    @Override
    public String toString() {
        return "MiruTopologyStatus{" +
            "partition=" + partition +
            ", lastIngressTimestamp=" + lastIngressTimestamp +
            ", lastQueryTimestamp=" + lastQueryTimestamp +
            ", destroyAfterTimestamp=" + destroyAfterTimestamp +
            ", cleanupAfterTimestamp=" + cleanupAfterTimestamp +
            ", lastId=" + lastId +
            ", lastIdTimestamp=" + lastIdTimestamp +
            '}';
    }
}
