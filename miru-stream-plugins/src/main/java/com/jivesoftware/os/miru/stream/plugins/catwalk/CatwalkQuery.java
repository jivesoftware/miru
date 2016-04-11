package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.io.Serializable;
import java.util.Arrays;

/**
 *
 */
public class CatwalkQuery implements Serializable {

    public final MiruTimeRange timeRange;
    public final String gatherField; // "parent"
    public final MiruFilter gatherFilter; // "I viewed"
    public final CatwalkFeature[] features;
    public final int desiredNumberOfResults;

    public CatwalkQuery(
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("gatherField") String gatherField,
        @JsonProperty("gatherFilter") MiruFilter gatherFilter,
        @JsonProperty("features") CatwalkFeature[] features,
        @JsonProperty("desiredNumberOfResults") int desiredNumberOfResults) {
        this.timeRange = Preconditions.checkNotNull(timeRange);
        this.gatherField = Preconditions.checkNotNull(gatherField);
        this.gatherFilter = Preconditions.checkNotNull(gatherFilter);
        this.features = Preconditions.checkNotNull(features);
        Preconditions.checkArgument(desiredNumberOfResults > 0, "Number of results must be at least 1");
        this.desiredNumberOfResults = desiredNumberOfResults;
    }

    @Override
    public String toString() {
        return "CatwalkQuery{"
            + "timeRange=" + timeRange
            + ", gatherField='" + gatherField + '\''
            + ", gatherFilter=" + gatherFilter
            + ", features=" + Arrays.toString(features)
            + ", desiredNumberOfResults=" + desiredNumberOfResults
            + '}';
    }

    public static class CatwalkFeature implements Serializable {

        public final String name;
        /**
         * {{ user }, { user, context }, { user, activityType }, { user, context, activityType }}
         * ["bob": 7 / 100], ["bob, water cooler": 2 / 6], ["bob, water cooler, created": 1 / 3]
         */
        public final String[] featureFields;
        public final MiruFilter featureFilter;

        @JsonCreator
        public CatwalkFeature(@JsonProperty("name") String name,
            @JsonProperty("featureFields") String[] featureFields,
            @JsonProperty("featureFilter") MiruFilter featureFilter) {
            this.name = name;
            this.featureFields = featureFields;
            this.featureFilter = featureFilter;
        }

        @Override
        public String toString() {
            return "CatwalkFeature{" +
                "name='" + name + '\'' +
                ", featureFields=" + Arrays.toString(featureFields) +
                ", featureFilter=" + featureFilter +
                '}';
        }
    }
}
