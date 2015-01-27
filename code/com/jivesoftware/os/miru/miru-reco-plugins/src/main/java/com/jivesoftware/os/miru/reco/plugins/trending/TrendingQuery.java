package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.io.Serializable;
import java.util.List;

/**
 *
 */
public class TrendingQuery implements Serializable {

    public final MiruTimeRange timeRange;
    public final int divideTimeRangeIntoNSegments;
    public final MiruFilter constraintsFilter;
    public final String aggregateCountAroundField;
    public final List<String> fieldPrefixes;
    public final int desiredNumberOfDistincts;

    @JsonCreator
    public TrendingQuery(
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("divideTimeRangeIntoNSegments") int divideTimeRangeIntoNSegments,
        @JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
        @JsonProperty("aggregateCountAroundField") String aggregateCountAroundField,
        @JsonProperty("fieldPrefixes") List<String> fieldPrefixes,
        @JsonProperty("desiredNumberOfDistincts") int desiredNumberOfDistincts) {
        Preconditions.checkArgument(!MiruTimeRange.ALL_TIME.equals(timeRange), "Requires an explicit time range");
        this.timeRange = Preconditions.checkNotNull(timeRange);
        Preconditions.checkArgument(divideTimeRangeIntoNSegments > 0, "Segments must be at least 1");
        this.divideTimeRangeIntoNSegments = divideTimeRangeIntoNSegments;
        this.constraintsFilter = Preconditions.checkNotNull(constraintsFilter);
        this.aggregateCountAroundField = Preconditions.checkNotNull(aggregateCountAroundField);
        this.fieldPrefixes = fieldPrefixes;
        Preconditions.checkArgument(desiredNumberOfDistincts > 0, "Number of distincts must be at least 1");
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    @Override
    public String toString() {
        return "TrendingQuery{" +
            "timeRange=" + timeRange +
            ", divideTimeRangeIntoNSegments=" + divideTimeRangeIntoNSegments +
            ", constraintsFilter=" + constraintsFilter +
            ", aggregateCountAroundField='" + aggregateCountAroundField + '\'' +
            ", fieldPrefixes=" + fieldPrefixes +
            ", desiredNumberOfDistincts=" + desiredNumberOfDistincts +
            '}';
    }
}
