package com.jivesoftware.os.miru.plugin.solution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

/**
 * @author jonathan
 */
public class MiruTimeRange implements Serializable {

    public static final MiruTimeRange ALL_TIME = new MiruTimeRange(0, Long.MAX_VALUE);

    public final long smallestTimestamp; // orderIds
    public final long largestTimestamp; // orderIds

    @JsonCreator
    public MiruTimeRange(
            @JsonProperty("smallestTimestamp") long smallestTimestamp,
            @JsonProperty("largestTimestamp") long largestTimestamp) {
        this.smallestTimestamp = smallestTimestamp;
        this.largestTimestamp = largestTimestamp;
    }

    @Override
    public String toString() {
        return "MiruTimeRange{" + "smallestTimestamp=" + smallestTimestamp + ", largestTimestamp=" + largestTimestamp + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MiruTimeRange timeRange = (MiruTimeRange) o;

        if (largestTimestamp != timeRange.largestTimestamp) {
            return false;
        }
        if (smallestTimestamp != timeRange.smallestTimestamp) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (smallestTimestamp ^ (smallestTimestamp >>> 32));
        result = 31 * result + (int) (largestTimestamp ^ (largestTimestamp >>> 32));
        return result;
    }

}
