package com.jivesoftware.os.miru.api.topology;

/**
 *
 */
public class MiruPartitionActive {

    public final long activeUntilTimestamp;
    public final long idleAfterTimestamp;
    public final long destroyAfterTimestamp;
    public final long cleanupAfterTimestamp;

    public MiruPartitionActive(long activeUntilTimestamp, long idleAfterTimestamp, long destroyAfterTimestamp, long cleanupAfterTimestamp) {
        this.activeUntilTimestamp = activeUntilTimestamp;
        this.idleAfterTimestamp = idleAfterTimestamp;
        this.destroyAfterTimestamp = destroyAfterTimestamp;
        this.cleanupAfterTimestamp = cleanupAfterTimestamp;
    }
}
