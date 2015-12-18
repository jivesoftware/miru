package com.jivesoftware.os.miru.analytics.plugins.analytics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.io.Serializable;

/**
 *
 */
public class AnalyticsQueryScoreSet implements Serializable {

    public final String key;
    public final MiruTimeRange timeRange;
    public final int divideTimeRangeIntoNSegments;

    @JsonCreator
    public AnalyticsQueryScoreSet(@JsonProperty("key") String key,
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("divideTimeRangeIntoNSegments") int divideTimeRangeIntoNSegments) {

        this.key = Preconditions.checkNotNull(key);

        Preconditions.checkArgument(!MiruTimeRange.ALL_TIME.equals(timeRange), "Requires an explicit time range");
        this.timeRange = Preconditions.checkNotNull(timeRange);

        Preconditions.checkArgument(divideTimeRangeIntoNSegments > 0, "Segments must be at least 1");
        this.divideTimeRangeIntoNSegments = divideTimeRangeIntoNSegments;
    }

    @Override
    public String toString() {
        return "AnalyticsQueryScoreSet{" +
            "key='" + key + '\'' +
            ", timeRange=" + timeRange +
            ", divideTimeRangeIntoNSegments=" + divideTimeRangeIntoNSegments +
            '}';
    }
}
