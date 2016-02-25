package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class CatwalkQuery {

    public final MiruTimeRange timeRange;
    public final MiruFilter constraintsFilter;
    public final String[][] featureFields; // {{ user }, { user, context }, { user, activityType }, { user, context, activityType }}
    // ["bob": 7 / 100], ["bob, water cooler": 2 / 6], ["bob, water cooler, created": 1 / 3]
    public final int desiredNumberOfResults;

    public CatwalkQuery(
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
        @JsonProperty("featureFields") String[][] featureFields,
        @JsonProperty("desiredNumberOfResults") int desiredNumberOfResults) {
        this.timeRange = Preconditions.checkNotNull(timeRange);
        this.constraintsFilter = Preconditions.checkNotNull(constraintsFilter);
        this.featureFields = Preconditions.checkNotNull(featureFields);
        Preconditions.checkArgument(desiredNumberOfResults > 0, "Number of results must be at least 1");
        this.desiredNumberOfResults = desiredNumberOfResults;
    }

    @Override
    public String toString() {
        return "CatwalkQuery{" +
            "timeRange=" + timeRange +
            ", constraintsFilter=" + constraintsFilter +
            ", featureFields=" + Arrays.toString(featureFields) +
            ", desiredNumberOfResults=" + desiredNumberOfResults +
            '}';
    }
}
