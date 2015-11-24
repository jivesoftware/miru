package com.jivesoftware.os.miru.stream.plugins.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.activity.MiruActivity;

/**
 * @author jonathan.colt
 */
public class AggregateCount {

    public final MiruActivity mostRecentActivity;
    public final String distinctValue;
    public final long count;
    public boolean unread;

    @JsonCreator
    public AggregateCount(
        @JsonProperty(value = "mostRecentActivity") MiruActivity mostRecentActivity,
        @JsonProperty(value = "distinctValue") String distinctValue,
        @JsonProperty(value = "count") long count,
        @JsonProperty(value = "unread") boolean unread) {
        this.mostRecentActivity = mostRecentActivity;
        this.distinctValue = distinctValue;
        this.count = count;
        this.unread = unread;
    }

    public void setUnread(boolean unread) {
        this.unread = unread;
    }

    @Override
    public String toString() {
        return "AggregateCount{" +
            "mostRecentActivity=" + mostRecentActivity +
            ", distinctValue=" + distinctValue +
            ", count=" + count +
            ", unread=" + unread + '}';
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
        if (unread != that.unread) {
            return false;
        }
        if (distinctValue != null ? !distinctValue.equals(that.distinctValue) : that.distinctValue != null) {
            return false;
        }
        if (mostRecentActivity != null ? !mostRecentActivity.equals(that.mostRecentActivity) : that.mostRecentActivity != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = mostRecentActivity != null ? mostRecentActivity.hashCode() : 0;
        result = 31 * result + (distinctValue != null ? distinctValue.hashCode() : 0);
        result = 31 * result + (int) (count ^ (count >>> 32));
        result = 31 * result + (unread ? 1 : 0);
        return result;
    }

}
