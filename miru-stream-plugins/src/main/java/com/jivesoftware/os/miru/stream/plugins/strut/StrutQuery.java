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
    public final String constraintField;
    public final MiruFilter constraintFilter;
    public final Strategy strategy;
    public final String[] featureNames;
    public final MiruFilter featureFilter;
    public final int desiredNumberOfResults;
    public final boolean includeFeatures;
    public final boolean usePartitionModelCache;

    public final String[] gatherTermsForFields;

    public StrutQuery(
        @JsonProperty("catwalkId") String catwalkId,
        @JsonProperty("modelId") String modelId,
        @JsonProperty("catwalkQuery") CatwalkQuery catwalkQuery,
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("constraintField") String constraintField,
        @JsonProperty("constraintFilter") MiruFilter constraintFilter,
        @JsonProperty("strategy") Strategy strategy,
        @JsonProperty("featureNames") String[] featureNames,
        @JsonProperty("featureFilter") MiruFilter featureFilter,
        @JsonProperty("desiredNumberOfResults") int desiredNumberOfResults,
        @JsonProperty("includeFeatures") boolean includeFeatures,
        @JsonProperty("usePartitionModelCache") boolean usePartitionModelCache,
        @JsonProperty("gatherTermsForFields") String[] gatherTermsForFields) {

        this.catwalkId = Preconditions.checkNotNull(catwalkId);
        this.modelId = Preconditions.checkNotNull(modelId);
        this.catwalkQuery = Preconditions.checkNotNull(catwalkQuery);
        this.timeRange = Preconditions.checkNotNull(timeRange);
        this.constraintField = Preconditions.checkNotNull(constraintField);
        this.constraintFilter = Preconditions.checkNotNull(constraintFilter);
        this.strategy = Preconditions.checkNotNull(strategy);
        this.featureNames = Preconditions.checkNotNull(featureNames);
        this.featureFilter = Preconditions.checkNotNull(featureFilter);
        Preconditions.checkArgument(desiredNumberOfResults > 0, "Number of results must be at least 1");
        this.desiredNumberOfResults = desiredNumberOfResults;
        this.includeFeatures = includeFeatures;
        this.usePartitionModelCache = usePartitionModelCache;
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
            ", featureNames=" + Arrays.toString(featureNames) +
            ", featureFilter=" + featureFilter +
            ", desiredNumberOfResults=" + desiredNumberOfResults +
            ", includeFeatures=" + includeFeatures +
            ", usePartitionModelCache=" + usePartitionModelCache +
            ", gatherTermsForFields=" + Arrays.toString(gatherTermsForFields) +
            '}';
    }

}
