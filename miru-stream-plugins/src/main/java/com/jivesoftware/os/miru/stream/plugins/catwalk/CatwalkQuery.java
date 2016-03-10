package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.util.Arrays;

/**
 *
 */
public class CatwalkQuery {

    public final MiruTimeRange timeRange;
    public final String gatherField; // "parent"
    public final MiruFilter gatherFilter; // "I viewed"
    /**
     * {{ user }, { user, context }, { user, activityType }, { user, context, activityType }}
     * ["bob": 7 / 100], ["bob, water cooler": 2 / 6], ["bob, water cooler, created": 1 / 3]
     */
    public final String[][] featureFields;
    public final MiruFilter featureFilter; // "I viewed"
    public final int desiredNumberOfResults;

    public CatwalkQuery(
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("gatherField") String gatherField,
        @JsonProperty("gatherFilter") MiruFilter gatherFilter,
        @JsonProperty("featureFields") String[][] featureFields,
        @JsonProperty("featureFilter") MiruFilter featureFilter,
        @JsonProperty("desiredNumberOfResults") int desiredNumberOfResults) {
        this.timeRange = Preconditions.checkNotNull(timeRange);
        this.gatherField = Preconditions.checkNotNull(gatherField);
        this.gatherFilter = Preconditions.checkNotNull(gatherFilter);
        this.featureFields = Preconditions.checkNotNull(featureFields);
        this.featureFilter = Preconditions.checkNotNull(featureFilter);
        Preconditions.checkArgument(desiredNumberOfResults > 0, "Number of results must be at least 1");
        this.desiredNumberOfResults = desiredNumberOfResults;
    }

    @Override
    public String toString() {
        return "CatwalkQuery{" +
            "timeRange=" + timeRange +
            ", gatherField='" + gatherField + '\'' +
            ", gatherFilter=" + gatherFilter +
            ", featureFields=" + Arrays.toString(featureFields) +
            ", featureFilter=" + featureFilter +
            ", desiredNumberOfResults=" + desiredNumberOfResults +
            '}';
    }
}
