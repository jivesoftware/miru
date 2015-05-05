package com.jivesoftware.os.miru.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MiruPartitionReplicaInfo {

    public final long electionTimestampMillis;
    public final long lastActiveTimestampMillis;
    public final MiruPartitionState state;
    public final MiruBackingStorage storage;

    @JsonCreator
    public MiruPartitionReplicaInfo(
        @JsonProperty("electionTimestampMillis") long electionTimestampMillis,
        @JsonProperty("lastActiveTimestampMillis") long lastActiveTimestampMillis,
        @JsonProperty("state") MiruPartitionState state,
        @JsonProperty("storage") MiruBackingStorage storage) {
        this.electionTimestampMillis = electionTimestampMillis;
        this.lastActiveTimestampMillis = lastActiveTimestampMillis;
        this.state = state;
        this.storage = storage;
    }

    @Override
    public String toString() {
        return "MiruPartitionCoordInfo{"
            + "electionTimestampMillis=" + electionTimestampMillis
            + "lastActiveTimestampMillis=" + lastActiveTimestampMillis
            + ", state=" + state
            + ", storage=" + storage
            + '}';
    }

}
