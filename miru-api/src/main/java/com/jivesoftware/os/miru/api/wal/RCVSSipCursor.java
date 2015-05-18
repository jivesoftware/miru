package com.jivesoftware.os.miru.api.wal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;

/**
 * @author jonathan.colt
 */
public class RCVSSipCursor implements MiruSipCursor<RCVSSipCursor> {

    public static final RCVSSipCursor INITIAL = new RCVSSipCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), 0, 0, false);

    public final byte sort;
    public final long clockTimestamp;
    public final long activityTimestamp;
    public final boolean endOfStream;

    @JsonCreator
    public RCVSSipCursor(@JsonProperty("sort") byte sort,
        @JsonProperty("clockTimestamp") long clockTimestamp,
        @JsonProperty("activityTimestamp") long activityTimestamp,
        @JsonProperty("endOfStream") boolean endOfStream) {
        this.sort = sort;
        this.clockTimestamp = clockTimestamp;
        this.activityTimestamp = activityTimestamp;
        this.endOfStream = endOfStream;
    }

    @Override
    public int compareTo(RCVSSipCursor o) {
        int c = Long.compare(clockTimestamp, o.clockTimestamp);
        if (c == 0) {
            c = Long.compare(activityTimestamp, o.activityTimestamp);
        }
        return c;
    }

    @Override
    public String toString() {
        return "RCVSSipCursor{" +
            "sort=" + sort +
            ", clockTimestamp=" + clockTimestamp +
            ", activityTimestamp=" + activityTimestamp +
            ", endOfStream=" + endOfStream +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RCVSSipCursor sipCursor = (RCVSSipCursor) o;

        if (sort != sipCursor.sort) {
            return false;
        }
        if (clockTimestamp != sipCursor.clockTimestamp) {
            return false;
        }
        if (activityTimestamp != sipCursor.activityTimestamp) {
            return false;
        }
        return endOfStream == sipCursor.endOfStream;

    }

    @Override
    public int hashCode() {
        int result = (int) sort;
        result = 31 * result + (int) (clockTimestamp ^ (clockTimestamp >>> 32));
        result = 31 * result + (int) (activityTimestamp ^ (activityTimestamp >>> 32));
        result = 31 * result + (endOfStream ? 1 : 0);
        return result;
    }
}
