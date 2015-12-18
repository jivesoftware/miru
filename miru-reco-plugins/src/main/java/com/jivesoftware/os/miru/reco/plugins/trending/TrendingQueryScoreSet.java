package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.util.Set;

/**
 *
 */
public class TrendingQueryScoreSet {

    public final String key;
    public final Set<TrendingQuery.Strategy> strategies;
    public final MiruTimeRange timeRange;
    public final int divideTimeRangeIntoNSegments;
    public final int desiredNumberOfDistincts;

    @JsonCreator
    public TrendingQueryScoreSet(@JsonProperty("key") String key,
        @JsonProperty("strategies") Set<TrendingQuery.Strategy> strategies,
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("divideTimeRangeIntoNSegments") int divideTimeRangeIntoNSegments,
        @JsonProperty("desiredNumberOfDistincts") int desiredNumberOfDistincts) {

        this.key = Preconditions.checkNotNull(key);

        Preconditions.checkArgument(strategies != null && !strategies.isEmpty(), "Must specify at least one strategy");
        this.strategies = strategies;

        Preconditions.checkArgument(!MiruTimeRange.ALL_TIME.equals(timeRange), "Requires an explicit time range");
        this.timeRange = Preconditions.checkNotNull(timeRange);

        this.divideTimeRangeIntoNSegments = divideTimeRangeIntoNSegments;

        Preconditions.checkArgument(desiredNumberOfDistincts > 0, "Number of distincts must be at least 1");
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    @Override
    public String toString() {
        return "TrendingQueryScoreSet{" +
            "key='" + key + '\'' +
            ", strategies=" + strategies +
            ", timeRange=" + timeRange +
            ", divideTimeRangeIntoNSegments=" + divideTimeRangeIntoNSegments +
            ", desiredNumberOfDistincts=" + desiredNumberOfDistincts +
            '}';
    }
}
