package com.jivesoftware.os.miru.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * @author jonathan
 */
public class MiruTimeRange {

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

}
