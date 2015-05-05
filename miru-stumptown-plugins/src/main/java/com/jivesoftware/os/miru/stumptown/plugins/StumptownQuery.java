package com.jivesoftware.os.miru.stumptown.plugins;

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
public class StumptownQuery implements Serializable {

    public final MiruTimeRange timeRange;
    public final int divideTimeRangeIntoNSegments;
    public final int desiredNumberOfResultsPerWaveform;
    public final MiruFilter constraintsFilter;
    public final Map<String, MiruFilter> stumptownFilters;

    @JsonCreator
    public StumptownQuery(
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("divideTimeRangeIntoNSegments") int divideTimeRangeIntoNSegments,
        @JsonProperty("desiredNumberOfResultsPerWaveform") int desiredNumberOfResultsPerWaveform,
        @JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
        @JsonProperty("stumptownFilters") Map<String, MiruFilter> stumptownFilters) {
        Preconditions.checkArgument(!MiruTimeRange.ALL_TIME.equals(timeRange), "Requires an explicit time range");
        this.timeRange = Preconditions.checkNotNull(timeRange);
        Preconditions.checkArgument(divideTimeRangeIntoNSegments > 0, "Segments must be at least 1");
        this.divideTimeRangeIntoNSegments = divideTimeRangeIntoNSegments;
        this.desiredNumberOfResultsPerWaveform = desiredNumberOfResultsPerWaveform;
        this.constraintsFilter = Preconditions.checkNotNull(constraintsFilter);
        this.stumptownFilters = Preconditions.checkNotNull(stumptownFilters);
    }

    @Override
    public String toString() {
        return "StumptownQuery{" +
            "timeRange=" + timeRange +
            ", divideTimeRangeIntoNSegments=" + divideTimeRangeIntoNSegments +
            ", desiredNumberOfResultsPerWaveform=" + desiredNumberOfResultsPerWaveform +
            ", constraintsFilter=" + constraintsFilter +
            ", stumptownFilters=" + stumptownFilters +
            '}';
    }
}
