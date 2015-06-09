package com.jivesoftware.os.miru.reco.plugins.uniques;

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
public class UniquesQuery implements Serializable {

    public final MiruTimeRange timeRange;
    public final String gatherUniquesForField;
    public final MiruFilter constraintsFilter;
    public final List<String> prefixes;

    @JsonCreator
    public UniquesQuery(
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("gatherUniquesForField") String gatherUniquesForField,
        @JsonProperty("constraints") MiruFilter constraintsFilter,
        @JsonProperty("prefixes") List<String> prefixes) {
        Preconditions.checkArgument(!MiruTimeRange.ALL_TIME.equals(timeRange), "Requires an explicit time range");
        this.timeRange = Preconditions.checkNotNull(timeRange);
        this.gatherUniquesForField = Preconditions.checkNotNull(gatherUniquesForField);
        this.prefixes = prefixes;
        this.constraintsFilter = constraintsFilter;
    }

    @Override
    public String toString() {
        return "UniquesQuery{" +
            "timeRange=" + timeRange +
            ", gatherUniquesForField='" + gatherUniquesForField + '\'' +
            ", constraintsFilter=" + constraintsFilter +
            ", prefixes=" + prefixes +
            '}';
    }
}
