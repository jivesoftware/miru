package com.jivesoftware.os.miru.plugin.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;

/**
 *
 */
public class TimeVersionRealtime {

    public final long timestamp;
    public final long version;
    public final boolean realtimeDelivery;

    @JsonCreator
    public TimeVersionRealtime(@JsonProperty("timestamp") long timestamp,
        @JsonProperty("version") long version,
        @JsonProperty("realtimeDelivery") boolean realtimeDelivery) {
        this.timestamp = timestamp;
        this.version = version;
        this.realtimeDelivery = realtimeDelivery;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimeVersionRealtime that = (TimeVersionRealtime) o;

        if (timestamp != that.timestamp) {
            return false;
        }
        if (version != that.version) {
            return false;
        }
        if (realtimeDelivery != that.realtimeDelivery) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (int) (version ^ (version >>> 32));
        result = 31 * result + (realtimeDelivery ? 1 : 0);
        return result;
    }
}
