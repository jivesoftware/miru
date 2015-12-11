package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class DistinctsQuery implements Serializable {

    public final MiruTimeRange timeRange;
    public final String gatherDistinctsForField;
    public final int[] gatherDistinctParts;
    public final MiruFilter constraintsFilter;
    public final List<MiruValue> prefixes;

    @JsonCreator
    public DistinctsQuery(
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("gatherDistinctsForField") String gatherDistinctsForField,
        @JsonProperty("gatherDistinctParts") int[] gatherDistinctParts,
        @JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
        @JsonProperty("prefixes") List<MiruValue> prefixes) {
        Preconditions.checkArgument(!MiruTimeRange.ALL_TIME.equals(timeRange), "Requires an explicit time range");
        this.timeRange = Preconditions.checkNotNull(timeRange);
        this.gatherDistinctsForField = Preconditions.checkNotNull(gatherDistinctsForField);
        this.gatherDistinctParts = gatherDistinctParts;
        this.prefixes = prefixes;
        this.constraintsFilter = constraintsFilter;
    }

    @Override
    public String toString() {
        return "DistinctsQuery{" +
            "timeRange=" + timeRange +
            ", gatherDistinctsForField='" + gatherDistinctsForField + '\'' +
            ", gatherDistinctParts=" + Arrays.toString(gatherDistinctParts) +
            ", constraintsFilter=" + constraintsFilter +
            ", prefixes=" + prefixes +
            '}';
    }
}
