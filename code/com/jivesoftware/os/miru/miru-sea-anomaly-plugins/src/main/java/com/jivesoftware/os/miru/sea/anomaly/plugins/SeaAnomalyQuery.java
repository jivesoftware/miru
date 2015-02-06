package com.jivesoftware.os.miru.sea.anomaly.plugins;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.io.Serializable;
import java.util.Map;

/**
 *
 */
public class SeaAnomalyQuery implements Serializable {

    public final MiruTimeRange timeRange;
    public final int divideTimeRangeIntoNSegments;
    public final MiruFilter constraintsFilter;
    public final Map<String, MiruFilter> filters;

    @JsonCreator
    public SeaAnomalyQuery(
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("divideTimeRangeIntoNSegments") int divideTimeRangeIntoNSegments,
        @JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
        @JsonProperty("filters") Map<String, MiruFilter> filters) {
        Preconditions.checkArgument(!MiruTimeRange.ALL_TIME.equals(timeRange), "Requires an explicit time range");
        this.timeRange = Preconditions.checkNotNull(timeRange);
        Preconditions.checkArgument(divideTimeRangeIntoNSegments > 0, "Segments must be at least 1");
        this.divideTimeRangeIntoNSegments = divideTimeRangeIntoNSegments;
        this.constraintsFilter = Preconditions.checkNotNull(constraintsFilter);
        this.filters = Preconditions.checkNotNull(filters);
    }

    @Override
    public String toString() {
        return "StumptownQuery{" +
            "timeRange=" + timeRange +
            ", divideTimeRangeIntoNSegments=" + divideTimeRangeIntoNSegments +
            ", constraintsFilter=" + constraintsFilter +
            ", stumptownFilters=" + filters +
            '}';
    }
}
