package com.jivesoftware.os.miru.api.topology;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class MiruPartitionActive {

    public final long lastIngressTimestamp;
    public final long activeUntilTimestamp;
    public final long idleAfterTimestamp;
    public final long destroyAfterTimestamp;
    public final long cleanupAfterTimestamp;

    @JsonCreator
    public MiruPartitionActive(@JsonProperty("lastIngressTimestamp") long lastIngressTimestamp,
        @JsonProperty("activeUntilTimestamp") long activeUntilTimestamp,
        @JsonProperty("idleAfterTimestamp") long idleAfterTimestamp,
        @JsonProperty("destroyAfterTimestamp") long destroyAfterTimestamp,
        @JsonProperty("cleanupAfterTimestamp") long cleanupAfterTimestamp) {
        this.lastIngressTimestamp = lastIngressTimestamp;
        this.activeUntilTimestamp = activeUntilTimestamp;
        this.idleAfterTimestamp = idleAfterTimestamp;
        this.destroyAfterTimestamp = destroyAfterTimestamp;
        this.cleanupAfterTimestamp = cleanupAfterTimestamp;
    }
}
