package com.jivesoftware.os.miru.api.activity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TimeAndVersion {

    public final long timestamp;
    public final long version;

    @JsonCreator
    public TimeAndVersion(@JsonProperty("timestamp") long timestamp,
        @JsonProperty("version") long version) {
        this.timestamp = timestamp;
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimeAndVersion that = (TimeAndVersion) o;

        if (timestamp != that.timestamp) {
            return false;
        }
        return version == that.version;
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (int) (version ^ (version >>> 32));
        return result;
    }
}
