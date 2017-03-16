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
    public final MiruValue[][] gatherOldestValues;
    public final long count;
    public final long latestTimestamp;
    public final long oldestTimestamp;
    public boolean anyUnread;
    public boolean latestUnread;
    public boolean oldestUnread;

    @JsonCreator
    public AggregateCount(
        @JsonProperty("distinctValue") MiruValue distinctValue,
        @JsonProperty("gatherLatestValues") MiruValue[][] gatherLatestValues,
        @JsonProperty("gatherOldestValues") MiruValue[][] gatherOldestValues,
        @JsonProperty("count") long count,
        @JsonProperty("latestTimestamp") long latestTimestamp,
        @JsonProperty("oldestTimestamp") long oldestTimestamp,
        @JsonProperty("anyUnread") boolean anyUnread,
        @JsonProperty("latestUnread") boolean latestUnread,
        @JsonProperty("oldestUnread") boolean oldestUnread) {
        this.distinctValue = distinctValue;
        this.gatherLatestValues = gatherLatestValues;
        this.gatherOldestValues = gatherOldestValues;
        this.count = count;
        this.latestTimestamp = latestTimestamp;
        this.oldestTimestamp = oldestTimestamp;
        this.anyUnread = anyUnread;
        this.latestUnread = latestUnread;
        this.oldestUnread = oldestUnread;
    }

    public void setLatestUnread(boolean latestUnread) {
        this.latestUnread = latestUnread;
    }

    public void setOldestUnread(boolean oldestUnread) {
        this.oldestUnread = oldestUnread;
    }

    @Override
    public String toString() {
        return "AggregateCount{" +
            "distinctValue=" + distinctValue +
            ", gatherLatestValues=" + Arrays.deepToString(gatherLatestValues) +
            ", gatherOldestValues=" + Arrays.deepToString(gatherOldestValues) +
            ", count=" + count +
            ", latestTimestamp=" + latestTimestamp +
            ", oldestTimestamp=" + oldestTimestamp +
            ", anyUnread=" + anyUnread +
            ", latestUnread=" + latestUnread +
            ", oldestUnread=" + oldestUnread +
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
