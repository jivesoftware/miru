package com.jivesoftware.os.miru.api.topology;

/**
 *
 */
public class MiruPartitionActive {

    public final long lastIngressTimestamp;
    public final long activeUntilTimestamp;
    public final long idleAfterTimestamp;
    public final long destroyAfterTimestamp;
    public final long cleanupAfterTimestamp;

    public MiruPartitionActive(long lastIngressTimestamp,
        long activeUntilTimestamp,
        long idleAfterTimestamp,
        long destroyAfterTimestamp,
        long cleanupAfterTimestamp) {
        this.lastIngressTimestamp = lastIngressTimestamp;
        this.activeUntilTimestamp = activeUntilTimestamp;
        this.idleAfterTimestamp = idleAfterTimestamp;
        this.destroyAfterTimestamp = destroyAfterTimestamp;
        this.cleanupAfterTimestamp = cleanupAfterTimestamp;
    }
}
