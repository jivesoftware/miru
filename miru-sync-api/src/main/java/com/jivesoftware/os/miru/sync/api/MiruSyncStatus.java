package com.jivesoftware.os.miru.sync.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class MiruSyncStatus {

    public final long forwardTimestamp;
    public final boolean forwardTaking;
    public final long reverseTimestamp;
    public final boolean reverseTaking;

    @JsonCreator
    public MiruSyncStatus(@JsonProperty("forwardTimestamp") long forwardTimestamp,
        @JsonProperty("forwardTaking") boolean forwardTaking,
        @JsonProperty("reverseTimestamp") long reverseTimestamp,
        @JsonProperty("reverseTaking") boolean reverseTaking) {
        this.forwardTimestamp = forwardTimestamp;
        this.forwardTaking = forwardTaking;
        this.reverseTimestamp = reverseTimestamp;
        this.reverseTaking = reverseTaking;
    }
}
