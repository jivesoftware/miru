package com.jivesoftware.os.miru.stream.plugins.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import java.util.Arrays;

/**
 * @author jonathan.colt
 */
public class AggregateCount {

    public final MiruValue distinctValue;
    public final MiruValue[][] gatherLatestValues;
    public final long count;
    public final long timestamp;
    public boolean unread;

    @JsonCreator
    public AggregateCount(
        @JsonProperty("distinctValue") MiruValue distinctValue,
        @JsonProperty("gatherLatestValues") MiruValue[][] gatherLatestValues,
        @JsonProperty("count") long count,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("unread") boolean unread) {
        this.distinctValue = distinctValue;
        this.gatherLatestValues = gatherLatestValues;
        this.count = count;
        this.timestamp = timestamp;
        this.unread = unread;
    }

    public void setUnread(boolean unread) {
        this.unread = unread;
    }

    @Override
    public String toString() {
        return "AggregateCount{" +
            "distinctValue=" + distinctValue +
            ", gatherLatestValues=" + Arrays.deepToString(gatherLatestValues) +
            ", count=" + count +
            ", timestamp=" + timestamp +
            ", unread=" + unread +
            '}';
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
