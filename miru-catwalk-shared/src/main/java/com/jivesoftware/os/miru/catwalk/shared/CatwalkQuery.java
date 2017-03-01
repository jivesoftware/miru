package com.jivesoftware.os.miru.catwalk.shared;

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

    public final String catwalkId;
    public final MiruTimeRange timeRange;
    public final String gatherField; // "parent"
    public final MiruFilter[] gatherFilters; // "I viewed"
    public final CatwalkFeature[] features;
    public final int desiredNumberOfResults;

    public CatwalkQuery(
        @JsonProperty("catwalkId") String catwalkId,
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("gatherField") String gatherField,
        @JsonProperty("gatherFilters") MiruFilter[] gatherFilters,
        @JsonProperty("features") CatwalkFeature[] features,
        @JsonProperty("desiredNumberOfResults") int desiredNumberOfResults) {
        this.catwalkId = catwalkId;
        this.timeRange = Preconditions.checkNotNull(timeRange);
        this.gatherField = Preconditions.checkNotNull(gatherField);
        this.gatherFilters = Preconditions.checkNotNull(gatherFilters);
        this.features = Preconditions.checkNotNull(features);
        Preconditions.checkArgument(desiredNumberOfResults > 0, "Number of results must be at least 1");
        this.desiredNumberOfResults = desiredNumberOfResults;
    }

    @Override
    public String toString() {
        return "CatwalkQuery{"
            + "catwalkId=" + catwalkId
            + ", timeRange=" + timeRange
            + ", gatherField='" + gatherField + '\''
            + ", gatherFilters=" + Arrays.toString(gatherFilters)
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
