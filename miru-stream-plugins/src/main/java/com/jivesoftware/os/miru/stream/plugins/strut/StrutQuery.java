package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.stream.plugins.catwalk.CatwalkQuery;
import java.io.Serializable;
import java.util.Arrays;

/**
 *
 */
public class StrutQuery implements Serializable {

    public enum Strategy {
        MAX,
        MEAN
    }

    public final String catwalkId;
    public final String modelId;
    public final CatwalkQuery catwalkQuery;

    public final MiruTimeRange timeRange;
    public final String constraintField; // "parent"
    public final MiruFilter constraintFilter; // "I viewed"
    public final Strategy strategy;
    /**
     * {{ user }, { user, context }, { user, activityType }, { user, context, activityType }}
     * ["bob": 7 / 100], ["bob, water cooler": 2 / 6], ["bob, water cooler, created": 1 / 3]
     */
    public final String[][] featureFields;
    public final MiruFilter featureFilter; // "I viewed"
    public final int desiredNumberOfResults;
    public final boolean includeFeatures;

    public final String[] gatherTermsForFields;

    public StrutQuery(
        @JsonProperty("catwalkId") String catwalkId,
        @JsonProperty("modelId") String modelId,
        @JsonProperty("catwalkQuery") CatwalkQuery catwalkQuery,
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("constraintField") String constraintField,
        @JsonProperty("constraintFilter") MiruFilter constraintFilter,
        @JsonProperty("strategy") Strategy strategy,
        @JsonProperty("featureFields") String[][] featureFields,
        @JsonProperty("featureFilter") MiruFilter featureFilter,
        @JsonProperty("desiredNumberOfResults") int desiredNumberOfResults,
        @JsonProperty("includeFeatures") boolean includeFeatures,
        @JsonProperty("gatherTermsForFields") String[] gatherTermsForFields) {

        this.catwalkId = Preconditions.checkNotNull(catwalkId);
        this.modelId = Preconditions.checkNotNull(modelId);
        this.catwalkQuery = Preconditions.checkNotNull(catwalkQuery);
        this.timeRange = Preconditions.checkNotNull(timeRange);
        this.constraintField = Preconditions.checkNotNull(constraintField);
        this.constraintFilter = Preconditions.checkNotNull(constraintFilter);
        this.strategy = Preconditions.checkNotNull(strategy);
        this.featureFields = Preconditions.checkNotNull(featureFields);
        this.featureFilter = Preconditions.checkNotNull(featureFilter);
        Preconditions.checkArgument(desiredNumberOfResults > 0, "Number of results must be at least 1");
        this.desiredNumberOfResults = desiredNumberOfResults;
        this.includeFeatures = includeFeatures;
        this.gatherTermsForFields = gatherTermsForFields;
    }

    @Override
    public String toString() {
        return "StrutQuery{" +
            "catwalkId='" + catwalkId + '\'' +
            ", modelId='" + modelId + '\'' +
            ", catwalkQuery=" + catwalkQuery +
            ", timeRange=" + timeRange +
            ", constraintField='" + constraintField + '\'' +
            ", constraintFilter=" + constraintFilter +
            ", strategy=" + strategy +
            ", featureFields=" + Arrays.toString(featureFields) +
            ", featureFilter=" + featureFilter +
            ", desiredNumberOfResults=" + desiredNumberOfResults +
            ", includeFeatures=" + includeFeatures +
            ", gatherTermsForFields=" + Arrays.toString(gatherTermsForFields) +
            '}';
    }

}
