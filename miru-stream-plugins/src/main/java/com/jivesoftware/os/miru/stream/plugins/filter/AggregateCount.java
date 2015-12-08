package com.jivesoftware.os.miru.stream.plugins.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;

/**
 * @author jonathan.colt
 */
public class AggregateCount {

    public final MiruActivity mostRecentActivity;
    public final MiruValue distinctValue;
    public final long count;
    public final long timestamp;
    public boolean unread;

    @JsonCreator
    public AggregateCount(
        @JsonProperty("mostRecentActivity") MiruActivity mostRecentActivity,
        @JsonProperty("distinctValue") MiruValue distinctValue,
        @JsonProperty("count") long count,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("unread") boolean unread) {
        this.mostRecentActivity = mostRecentActivity;
        this.distinctValue = distinctValue;
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
            "mostRecentActivity=" + mostRecentActivity +
            ", distinctValue='" + distinctValue + '\'' +
            ", count=" + count +
            ", timestamp=" + timestamp +
            ", unread=" + unread +
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

        AggregateCount that = (AggregateCount) o;

        if (count != that.count) {
            return false;
        }
        if (timestamp != that.timestamp) {
            return false;
        }
        if (unread != that.unread) {
            return false;
        }
        if (mostRecentActivity != null ? !mostRecentActivity.equals(that.mostRecentActivity) : that.mostRecentActivity != null) {
            return false;
        }
        return !(distinctValue != null ? !distinctValue.equals(that.distinctValue) : that.distinctValue != null);

    }

    @Override
    public int hashCode() {
        int result = mostRecentActivity != null ? mostRecentActivity.hashCode() : 0;
        result = 31 * result + (distinctValue != null ? distinctValue.hashCode() : 0);
        result = 31 * result + (int) (count ^ (count >>> 32));
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (unread ? 1 : 0);
        return result;
    }
}
