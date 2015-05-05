package com.jivesoftware.os.miru.analytics.plugins.metrics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.io.Serializable;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
 */
public class MetricsQuery implements Serializable {

    public final MiruTimeRange timeRange;
    public final int divideTimeRangeIntoNSegments;
    public final String powerBitsFieldName;
    public final MiruFilter constraintsFilter;
    public final Map<String, MiruFilter> filters;

    @JsonCreator
    public MetricsQuery(
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("divideTimeRangeIntoNSegments") int divideTimeRangeIntoNSegments,
        @JsonProperty("powerBitsFieldName") String powerBitsFieldName,
        @JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
        @JsonProperty("filters") Map<String, MiruFilter> filters) {
        Preconditions.checkArgument(!MiruTimeRange.ALL_TIME.equals(timeRange), "Requires an explicit time range");
        this.timeRange = checkNotNull(timeRange);
        Preconditions.checkArgument(divideTimeRangeIntoNSegments > 0, "Segments must be at least 1");
        this.divideTimeRangeIntoNSegments = divideTimeRangeIntoNSegments;
        this.powerBitsFieldName = checkNotNull(powerBitsFieldName);
        this.constraintsFilter = checkNotNull(constraintsFilter);
        this.filters = checkNotNull(filters);
    }

    @Override
    public String toString() {
        return "MetricsQuery{" +
            "timeRange=" + timeRange +
            ", divideTimeRangeIntoNSegments=" + divideTimeRangeIntoNSegments +
            ", powerBitsFieldName='" + powerBitsFieldName + '\'' +
            ", constraintsFilter=" + constraintsFilter +
            ", filters=" + filters +
            '}';
    }
}
