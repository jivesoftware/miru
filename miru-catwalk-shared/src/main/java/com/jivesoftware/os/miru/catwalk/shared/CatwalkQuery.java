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

    public final CatwalkDefinition definition;
    public final CatwalkModelQuery modelQuery;

    @JsonCreator
    public CatwalkQuery(
        @JsonProperty("definition") CatwalkDefinition definition,
        @JsonProperty("modelQuery") CatwalkModelQuery modelQuery) {
        Preconditions.checkArgument(definition.numeratorCount == modelQuery.modelFilters.length, "Numerator count must equal model filters length");
        this.definition = Preconditions.checkNotNull(definition);
        this.modelQuery = Preconditions.checkNotNull(modelQuery);
    }

    @Override
    public String toString() {
        return "CatwalkQuery{" +
            "definition=" + definition +
            ", modelQuery=" + modelQuery +
            '}';
    }

    public static class CatwalkDefinition implements Serializable {

        public final String catwalkId;
        public final String gatherField; // "parent"
        public final String scorableField;
        public final CatwalkFeature[] features;
        public final Strategy featureStrategy;
        public final MiruFilter scorableFilter; // "content types and activity types"
        public final int numeratorCount;

        @JsonCreator
        public CatwalkDefinition(@JsonProperty("catwalkId") String catwalkId,
            @JsonProperty("gatherField") String gatherField,
            @JsonProperty("scorableField") String scorableField,
            @JsonProperty("features") CatwalkFeature[] features,
            @JsonProperty("featureStrategy") Strategy featureStrategy,
            @JsonProperty("scorableFilter") MiruFilter scorableFilter,
            @JsonProperty("numeratorCount") int numeratorCount) {
            this.catwalkId = catwalkId;
            this.gatherField = gatherField;
            this.scorableField = scorableField;
            this.features = features;
            this.featureStrategy = featureStrategy;
            this.scorableFilter = scorableFilter;
            this.numeratorCount = numeratorCount;
        }

        @Override
        public String toString() {
            return "CatwalkDefinition{" +
                "catwalkId='" + catwalkId + '\'' +
                ", gatherField='" + gatherField + '\'' +
                ", scorableField='" + scorableField + '\'' +
                ", features=" + Arrays.toString(features) +
                ", featureStrategy=" + featureStrategy +
                ", scorableFilter=" + scorableFilter +
                ", numeratorCount=" + numeratorCount +
                '}';
        }
    }

    public static class CatwalkFeature implements Serializable {

        public final String name;
        /**
         * {{ user }, { user, context }, { user, activityType }, { user, context, activityType }}
         * ["bob": 7 / 100], ["bob, water cooler": 2 / 6], ["bob, water cooler, created": 1 / 3]
         */
        public final String[] featureFields;
        public final MiruFilter featureFilter;
        public final float featureScalar;

        @JsonCreator
        public CatwalkFeature(@JsonProperty("name") String name,
            @JsonProperty("featureFields") String[] featureFields,
            @JsonProperty("featureFilter") MiruFilter featureFilter,
            @JsonProperty("featureScalar") float featureScalar) {
            this.name = name;
            this.featureFields = featureFields;
            this.featureFilter = featureFilter;
            this.featureScalar = featureScalar;
        }

        @Override
        public String toString() {
            return "CatwalkFeature{" +
                "name='" + name + '\'' +
                ", featureFields=" + Arrays.toString(featureFields) +
                ", featureFilter=" + featureFilter +
                ", featureScalar=" + featureScalar +
                '}';
        }
    }

    public static class CatwalkModelQuery implements Serializable {

        public final MiruFilter[] modelFilters; // "I viewed"
        public final MiruTimeRange timeRange;
        public final int desiredNumberOfResults;

        @JsonCreator
        public CatwalkModelQuery(
            @JsonProperty("timeRange") MiruTimeRange timeRange,
            @JsonProperty("modelFilters") MiruFilter[] modelFilters,
            @JsonProperty("desiredNumberOfResults") int desiredNumberOfResults) {
            this.timeRange = Preconditions.checkNotNull(timeRange);
            this.modelFilters = Preconditions.checkNotNull(modelFilters);
            Preconditions.checkArgument(desiredNumberOfResults > 0, "Number of results must be at least 1");
            this.desiredNumberOfResults = desiredNumberOfResults;
        }

        @Override
        public String toString() {
            return "CatwalkModelQuery{" +
                "modelFilters=" + Arrays.toString(modelFilters) +
                ", timeRange=" + timeRange +
                ", desiredNumberOfResults=" + desiredNumberOfResults +
                '}';
        }
    }
}
