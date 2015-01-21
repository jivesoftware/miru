package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.io.Serializable;

/**
 *
 */
public class DistinctsQuery implements Serializable {

    public final MiruTimeRange timeRange;
    public final MiruFilter constraintsFilter;
    public final String distinctsAroundField;

    @JsonCreator
    public DistinctsQuery(
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
        @JsonProperty("distinctsAroundField") String distinctsAroundField) {
        Preconditions.checkArgument(!MiruTimeRange.ALL_TIME.equals(timeRange), "Requires an explicit time range");
        this.timeRange = Preconditions.checkNotNull(timeRange);
        this.constraintsFilter = Preconditions.checkNotNull(constraintsFilter);
        this.distinctsAroundField = Preconditions.checkNotNull(distinctsAroundField);
    }

    @Override
    public String toString() {
        return "DistinctsQuery{" +
            "timeRange=" + timeRange +
            ", constraintsFilter=" + constraintsFilter +
            ", distinctsAroundField='" + distinctsAroundField + '\'' +
            '}';
    }
}
