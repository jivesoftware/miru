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
        UNIT_WEIGHTED, // S = mean(A,B,C,D)
        REGRESSION_WEIGHTED, // S = 0.5*A + 0.4*B + 0.4*C + 0.3*D
        MAX
    }

    public final String catwalkId;
    public final String modelId;
    public final CatwalkQuery catwalkQuery;

    public final MiruTimeRange timeRange;
    public final String constraintField;
    public final MiruFilter constraintFilter;
    public final Strategy numeratorStrategy;
    public final float[] numeratorScalars;
    public final Strategy featureStrategy;
    public final float[] featureScalars;
    public final MiruFilter featureFilter;
    public final int desiredNumberOfResults;
    public final boolean includeFeatures;
    public final boolean usePartitionModelCache;

    public final String[] gatherTermsForFields;
    public final int batchSize;

    public StrutQuery(
        @JsonProperty("catwalkId") String catwalkId,
        @JsonProperty("modelId") String modelId,
        @JsonProperty("catwalkQuery") CatwalkQuery catwalkQuery,
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("constraintField") String constraintField,
        @JsonProperty("constraintFilter") MiruFilter constraintFilter,
        @JsonProperty("numeratorStrategy") Strategy numeratorStrategy,
        @JsonProperty("numeratorScalars") float[] numeratorScalars,
        @JsonProperty("featureStrategy") Strategy featureStrategy,
        @JsonProperty("featureScalars") float[] featureScalars,
        @JsonProperty("featureFilter") MiruFilter featureFilter,
        @JsonProperty("desiredNumberOfResults") int desiredNumberOfResults,
        @JsonProperty("includeFeatures") boolean includeFeatures,
        @JsonProperty("usePartitionModelCache") boolean usePartitionModelCache,
        @JsonProperty("gatherTermsForFields") String[] gatherTermsForFields,
        @JsonProperty("batchSize") int batchSize) {

        this.catwalkId = Preconditions.checkNotNull(catwalkId);
        this.modelId = Preconditions.checkNotNull(modelId);
        this.catwalkQuery = Preconditions.checkNotNull(catwalkQuery);
        this.timeRange = Preconditions.checkNotNull(timeRange);
        this.constraintField = Preconditions.checkNotNull(constraintField);
        this.constraintFilter = Preconditions.checkNotNull(constraintFilter);
        this.numeratorStrategy = Preconditions.checkNotNull(numeratorStrategy);
        Preconditions.checkArgument(numeratorScalars.length == catwalkQuery.gatherFilters.length,
            "numeratorScalars must be the same length as catwalkQuery.gatherFilters");
        this.numeratorScalars = numeratorScalars;
        this.featureStrategy = Preconditions.checkNotNull(featureStrategy);
        Preconditions.checkArgument(featureScalars.length == catwalkQuery.features.length,
            "featureScalars must be the same length as catwalkQuery.features");
        this.featureScalars = featureScalars;
        this.featureFilter = Preconditions.checkNotNull(featureFilter);
        Preconditions.checkArgument(desiredNumberOfResults > 0, "Number of results must be at least 1");
        this.desiredNumberOfResults = desiredNumberOfResults;
        this.includeFeatures = includeFeatures;
        this.usePartitionModelCache = usePartitionModelCache;
        this.gatherTermsForFields = gatherTermsForFields;
        this.batchSize = batchSize;
    }

    @Override
    public String toString() {
        return "StrutQuery{"
            + "catwalkId='" + catwalkId + '\''
            + ", modelId='" + modelId + '\''
            + ", catwalkQuery=" + catwalkQuery
            + ", timeRange=" + timeRange
            + ", constraintField='" + constraintField + '\''
            + ", constraintFilter=" + constraintFilter
            + ", numeratorStrategy=" + numeratorStrategy
            + ", numeratorScalars=" + Arrays.toString(numeratorScalars)
            + ", featureStrategy=" + featureStrategy
            + ", featureScalars=" + Arrays.toString(featureScalars)
            + ", featureFilter=" + featureFilter
            + ", desiredNumberOfResults=" + desiredNumberOfResults
            + ", includeFeatures=" + includeFeatures
            + ", usePartitionModelCache=" + usePartitionModelCache
            + ", gatherTermsForFields=" + Arrays.toString(gatherTermsForFields)
            + ", batchSize=" + batchSize
            + '}';
    }

}
