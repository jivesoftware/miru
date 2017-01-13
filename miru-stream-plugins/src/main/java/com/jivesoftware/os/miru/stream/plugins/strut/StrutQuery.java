package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class StrutQuery implements Serializable {

    public enum Strategy {
        UNIT_WEIGHTED, // S = mean(A,B,C,D)
        REGRESSION_WEIGHTED, // S = 0.5*A + 0.4*B + 0.4*C + 0.3*D
        MAX
    }

    public final List<StrutModelScalar> modelScalars;

    public final MiruTimeRange timeRange;
    public final String constraintField;
    public final MiruFilter constraintFilter;
    public final Strategy numeratorStrategy;
    public final float[] numeratorScalars;
    public final Strategy featureStrategy;
    public final float[] featureScalars;
    public final int desiredNumberOfResults;
    public final boolean includeFeatures;
    public final boolean usePartitionModelCache;

    public final String[] gatherTermsForFields;
    public final MiruStreamId unreadStreamId;
    public final MiruFilter suppressUnreadFilter;
    public final boolean unreadOnly;
    public final int batchSize;

    public StrutQuery(
        @JsonProperty("modelScalars") List<StrutModelScalar> modelScalars,
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("constraintField") String constraintField,
        @JsonProperty("constraintFilter") MiruFilter constraintFilter,
        @JsonProperty("numeratorStrategy") Strategy numeratorStrategy,
        @JsonProperty("numeratorScalars") float[] numeratorScalars,
        @JsonProperty("featureStrategy") Strategy featureStrategy,
        @JsonProperty("featureScalars") float[] featureScalars,
        @JsonProperty("desiredNumberOfResults") int desiredNumberOfResults,
        @JsonProperty("includeFeatures") boolean includeFeatures,
        @JsonProperty("usePartitionModelCache") boolean usePartitionModelCache,
        @JsonProperty("gatherTermsForFields") String[] gatherTermsForFields,
        @JsonProperty("unreadStreamId") MiruStreamId unreadStreamId, // nullable
        @JsonProperty("suppressUnreadFilter") MiruFilter suppressUnreadFilter, // nullable
        @JsonProperty("unreadOnly") boolean unreadOnly,
        @JsonProperty("batchSize") int batchSize) {

        this.modelScalars = Preconditions.checkNotNull(modelScalars);
        Preconditions.checkArgument(modelScalars.size() > 0);

        this.timeRange = Preconditions.checkNotNull(timeRange);
        this.constraintField = Preconditions.checkNotNull(constraintField);
        this.constraintFilter = Preconditions.checkNotNull(constraintFilter);
        this.numeratorStrategy = Preconditions.checkNotNull(numeratorStrategy);

        for (StrutModelScalar modelScalar : modelScalars) {
            Preconditions.checkArgument(numeratorScalars.length == modelScalar.catwalkQuery.gatherFilters.length,
                "numeratorScalars must be the same length as catwalkQuery.gatherFilters");
        }
        this.numeratorScalars = numeratorScalars;
        this.featureStrategy = Preconditions.checkNotNull(featureStrategy);
        for (StrutModelScalar modelScalar : modelScalars) {
            Preconditions.checkArgument(featureScalars.length == modelScalar.catwalkQuery.features.length,
                "featureScalars must be the same length as catwalkQuery.features");
        }
        this.featureScalars = featureScalars;
        Preconditions.checkArgument(desiredNumberOfResults > 0, "Number of results must be at least 1");
        this.desiredNumberOfResults = desiredNumberOfResults;
        this.includeFeatures = includeFeatures;
        this.usePartitionModelCache = usePartitionModelCache;
        this.gatherTermsForFields = gatherTermsForFields;
        this.unreadStreamId = unreadStreamId;
        this.suppressUnreadFilter = suppressUnreadFilter;
        this.unreadOnly = unreadOnly;
        this.batchSize = batchSize;
    }

    @Override
    public String toString() {
        return "StrutQuery{" +
            "modelScalars=" + modelScalars +
            ", timeRange=" + timeRange +
            ", constraintField='" + constraintField + '\'' +
            ", constraintFilter=" + constraintFilter +
            ", numeratorStrategy=" + numeratorStrategy +
            ", numeratorScalars=" + Arrays.toString(numeratorScalars) +
            ", featureStrategy=" + featureStrategy +
            ", featureScalars=" + Arrays.toString(featureScalars) +
            ", desiredNumberOfResults=" + desiredNumberOfResults +
            ", includeFeatures=" + includeFeatures +
            ", usePartitionModelCache=" + usePartitionModelCache +
            ", gatherTermsForFields=" + Arrays.toString(gatherTermsForFields) +
            ", unreadStreamId=" + unreadStreamId +
            ", suppressUnreadFilter=" + suppressUnreadFilter +
            ", unreadOnly=" + unreadOnly +
            ", batchSize=" + batchSize +
            '}';
    }

}
