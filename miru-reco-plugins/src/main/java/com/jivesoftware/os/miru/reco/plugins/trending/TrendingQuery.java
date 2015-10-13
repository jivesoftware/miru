package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class TrendingQuery implements Serializable {

    public static enum Strategy {

        LINEAR_REGRESSION, LEADER, PEAKS, HIGHEST_PEAK;
    }

    public final Set<Strategy> strategies;
    public final MiruTimeRange timeRange;
    public final MiruTimeRange relativeChangeTimeRange; // nullable
    public final int divideTimeRangeIntoNSegments;
    public final MiruFilter constraintsFilter;
    public final String aggregateCountAroundField;
    public final List<DistinctsQuery> distinctQueries;
    public final int desiredNumberOfDistincts;

    @JsonCreator
    public TrendingQuery(
        @JsonProperty("strategies") Set<Strategy> strategies,
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("relativeChangeTimeRange") MiruTimeRange relativeChangeTimeRange,
        @JsonProperty("divideTimeRangeIntoNSegments") int divideTimeRangeIntoNSegments,
        @JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
        @JsonProperty("aggregateCountAroundField") String aggregateCountAroundField,
        @JsonProperty("distinctQueries") List<DistinctsQuery> distinctQueries,
        @JsonProperty("desiredNumberOfDistincts") int desiredNumberOfDistincts) {
        Preconditions.checkArgument(strategies != null && !strategies.isEmpty(), "Must specify at least one strategy");
        this.strategies = strategies;

        Preconditions.checkArgument(!MiruTimeRange.ALL_TIME.equals(timeRange), "Requires an explicit time range");
        this.timeRange = Preconditions.checkNotNull(timeRange);
        this.relativeChangeTimeRange = relativeChangeTimeRange;

        this.divideTimeRangeIntoNSegments = divideTimeRangeIntoNSegments;
        this.constraintsFilter = Preconditions.checkNotNull(constraintsFilter);
        this.aggregateCountAroundField = Preconditions.checkNotNull(aggregateCountAroundField);
        this.distinctQueries = Preconditions.checkNotNull(distinctQueries);
        Preconditions.checkArgument(desiredNumberOfDistincts > 0, "Number of distincts must be at least 1");
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    @Override
    public String toString() {
        return "TrendingQuery{"
            + "strategies=" + strategies
            + ", timeRange=" + timeRange
            + ", relativeChangeTimeRange=" + relativeChangeTimeRange
            + ", divideTimeRangeIntoNSegments=" + divideTimeRangeIntoNSegments
            + ", constraintsFilter=" + constraintsFilter
            + ", aggregateCountAroundField='" + aggregateCountAroundField + '\''
            + ", distinctQueries=" + distinctQueries
            + ", desiredNumberOfDistincts=" + desiredNumberOfDistincts
            + '}';
    }
}
