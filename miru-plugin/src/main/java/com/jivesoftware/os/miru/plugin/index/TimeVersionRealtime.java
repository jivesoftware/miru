package com.jivesoftware.os.miru.plugin.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class TimeVersionRealtime {

    public final long timestamp;
    public final long version;
    public final long monoTimestamp;
    public final boolean realtimeDelivery;

    @JsonCreator
    public TimeVersionRealtime(@JsonProperty("timestamp") long timestamp,
        @JsonProperty("version") long version,
        @JsonProperty("monoTimestamp") long monoTimestamp,
        @JsonProperty("realtimeDelivery") boolean realtimeDelivery) {
        this.timestamp = timestamp;
        this.version = version;
        this.monoTimestamp = monoTimestamp;
        this.realtimeDelivery = realtimeDelivery;
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException("NOPE");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("NOPE");
    }
}
