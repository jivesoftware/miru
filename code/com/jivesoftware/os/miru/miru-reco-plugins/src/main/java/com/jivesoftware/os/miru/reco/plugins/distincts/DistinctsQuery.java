package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.io.Serializable;

/**
 *
 */
public class DistinctsQuery implements Serializable {

    public final MiruTimeRange timeRange;
    public final String aggregateCountAroundField;

    @JsonCreator
    public DistinctsQuery(
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("aggregateCountAroundField") String aggregateCountAroundField) {
        Preconditions.checkArgument(!MiruTimeRange.ALL_TIME.equals(timeRange), "Requires an explicit time range");
        this.timeRange = Preconditions.checkNotNull(timeRange);
        this.aggregateCountAroundField = Preconditions.checkNotNull(aggregateCountAroundField);
    }

    @Override
    public String toString() {
        return "DistinctsQuery{" +
                "timeRange=" + timeRange +
                ", aggregateCountAroundField='" + aggregateCountAroundField + '\'' +
                '}';
    }
}
