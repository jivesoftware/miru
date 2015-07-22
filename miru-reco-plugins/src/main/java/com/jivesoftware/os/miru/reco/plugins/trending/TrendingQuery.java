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

    public static enum Strategy {

        LINEAR_REGRESSION, LEADER, PEAKS, HIGHEST_PEAK;
    }

    public final Strategy strategy;
    public final MiruTimeRange timeRange;
    public final MiruTimeRange relativeChangeTimeRange; // nullable
    public final int divideTimeRangeIntoNSegments;
    public final MiruFilter constraintsFilter;
    public final String aggregateCountAroundField;
    public final MiruFilter distinctsFilter;
    public final List<String> distinctPrefixes;
    public final int desiredNumberOfDistincts;

    @JsonCreator
    public TrendingQuery(
        @JsonProperty("strategy") Strategy strategy,
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("relativeChangeTimeRange") MiruTimeRange relativeChangeTimeRange,
        @JsonProperty("divideTimeRangeIntoNSegments") int divideTimeRangeIntoNSegments,
        @JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
        @JsonProperty("aggregateCountAroundField") String aggregateCountAroundField,
        @JsonProperty("distinctsFilter") MiruFilter distinctsFilter,
        @JsonProperty("distinctPrefixes") List<String> distinctPrefixes,
        @JsonProperty("desiredNumberOfDistincts") int desiredNumberOfDistincts) {
        Preconditions.checkNotNull(strategy, "Must specifiy a strategy");
        this.strategy = strategy;

        Preconditions.checkArgument(!MiruTimeRange.ALL_TIME.equals(timeRange), "Requires an explicit time range");
        this.timeRange = Preconditions.checkNotNull(timeRange);
        this.relativeChangeTimeRange = relativeChangeTimeRange;

        if (strategy == Strategy.LINEAR_REGRESSION) {
            Preconditions.checkArgument(divideTimeRangeIntoNSegments > 1, "Strategy.LINEAR_REGRESSION requires divideTimeRangeIntoNSegments > 1");
        } else if (strategy == Strategy.PEAKS) {
            Preconditions.checkArgument(divideTimeRangeIntoNSegments > 1, "Strategy.PEAKS requires divideTimeRangeIntoNSegments > 1");
        } else if (strategy == Strategy.LEADER) {
            Preconditions.checkArgument(divideTimeRangeIntoNSegments == 1, "Strategy.LEADER requires divideTimeRangeIntoNSegments == 1");
        }
        this.divideTimeRangeIntoNSegments = divideTimeRangeIntoNSegments;
        this.constraintsFilter = Preconditions.checkNotNull(constraintsFilter);
        this.aggregateCountAroundField = Preconditions.checkNotNull(aggregateCountAroundField);
        this.distinctsFilter = Preconditions.checkNotNull(distinctsFilter);
        this.distinctPrefixes = distinctPrefixes;
        Preconditions.checkArgument(desiredNumberOfDistincts > 0, "Number of distincts must be at least 1");
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    @Override
    public String toString() {
        return "TrendingQuery{"
            + "strategy=" + strategy
            + ", timeRange=" + timeRange
            + ", relativeChangeTimeRange=" + relativeChangeTimeRange
            + ", divideTimeRangeIntoNSegments=" + divideTimeRangeIntoNSegments
            + ", constraintsFilter=" + constraintsFilter
            + ", aggregateCountAroundField='" + aggregateCountAroundField + '\''
            + ", distinctsFilter=" + distinctsFilter
            + ", distinctPrefixes=" + distinctPrefixes
            + ", desiredNumberOfDistincts=" + desiredNumberOfDistincts
            + '}';
    }
}
