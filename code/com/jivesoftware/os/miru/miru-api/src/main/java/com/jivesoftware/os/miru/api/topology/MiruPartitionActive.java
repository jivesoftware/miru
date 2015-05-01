package com.jivesoftware.os.miru.api.topology;

/**
 *
 */
public class MiruPartitionActive {

    public final long activeUntilTimestamp;
    public final long idleAfterTimestamp;

    public MiruPartitionActive(long activeUntilTimestamp, long idleAfterTimestamp) {
        this.activeUntilTimestamp = activeUntilTimestamp;
        this.idleAfterTimestamp = idleAfterTimestamp;
    }
}
