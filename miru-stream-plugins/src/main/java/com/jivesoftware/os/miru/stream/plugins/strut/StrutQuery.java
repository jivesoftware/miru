package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.catwalk.shared.CatwalkQuery.CatwalkDefinition;
import com.jivesoftware.os.miru.catwalk.shared.Strategy;
import com.jivesoftware.os.miru.catwalk.shared.StrutModelScalar;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StrutQuery implements Serializable {

    public final CatwalkDefinition catwalkDefinition;
    public final List<StrutModelScalar> modelScalars;

    public final MiruTimeRange timeRange;
    public final MiruFilter constraintFilter;
    public final Strategy numeratorStrategy;
    public final float[] numeratorScalars;
    public final int desiredNumberOfResults;
    public final boolean includeFeatures;
    public final String[] gatherTermsForFields;
    public final MiruStreamId unreadStreamId;
    public final MiruFilter suppressUnreadFilter;
    public final boolean unreadOnly;
    public final boolean countUnread;
    public final int batchSize;

    public StrutQuery(
        @JsonProperty("catwalkDefinition") CatwalkDefinition catwalkDefinition,
        @JsonProperty("modelScalars") List<StrutModelScalar> modelScalars,
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("constraintFilter") MiruFilter constraintFilter,
        @JsonProperty("numeratorStrategy") Strategy numeratorStrategy,
        @JsonProperty("numeratorScalars") float[] numeratorScalars,
        @JsonProperty("desiredNumberOfResults") int desiredNumberOfResults,
        @JsonProperty("includeFeatures") boolean includeFeatures,
        @JsonProperty("gatherTermsForFields") String[] gatherTermsForFields,
        @JsonProperty("unreadStreamId") MiruStreamId unreadStreamId, // nullable
        @JsonProperty("suppressUnreadFilter") MiruFilter suppressUnreadFilter, // nullable
        @JsonProperty("unreadOnly") boolean unreadOnly,
        @JsonProperty("countUnread") boolean countUnread,
        @JsonProperty("batchSize") int batchSize) {
        this.catwalkDefinition = catwalkDefinition;

        this.modelScalars = Preconditions.checkNotNull(modelScalars);
        Preconditions.checkArgument(modelScalars.size() > 0);

        this.timeRange = Preconditions.checkNotNull(timeRange);
        this.constraintFilter = Preconditions.checkNotNull(constraintFilter);
        this.numeratorStrategy = Preconditions.checkNotNull(numeratorStrategy);

        for (StrutModelScalar modelScalar : modelScalars) {
            Preconditions.checkArgument(numeratorScalars.length == modelScalar.catwalkModelQuery.modelFilters.length,
                "numeratorScalars must be the same length as catwalkModelQuery.modelFilters");
        }
        this.numeratorScalars = numeratorScalars;
        Preconditions.checkArgument(desiredNumberOfResults > 0, "Number of results must be at least 1");
        this.desiredNumberOfResults = desiredNumberOfResults;
        this.includeFeatures = includeFeatures;
        this.gatherTermsForFields = gatherTermsForFields;
        this.unreadStreamId = unreadStreamId;
        this.suppressUnreadFilter = suppressUnreadFilter;
        this.unreadOnly = unreadOnly;
        this.countUnread = countUnread;
        this.batchSize = batchSize;
    }

    @Override
    public String toString() {
        return "StrutQuery{" +
            "catwalkDefinition=" + catwalkDefinition +
            ", modelScalars=" + modelScalars +
            ", timeRange=" + timeRange +
            ", constraintFilter=" + constraintFilter +
            ", numeratorStrategy=" + numeratorStrategy +
            ", numeratorScalars=" + Arrays.toString(numeratorScalars) +
            ", desiredNumberOfResults=" + desiredNumberOfResults +
            ", includeFeatures=" + includeFeatures +
            ", gatherTermsForFields=" + Arrays.toString(gatherTermsForFields) +
            ", unreadStreamId=" + unreadStreamId +
            ", suppressUnreadFilter=" + suppressUnreadFilter +
            ", unreadOnly=" + unreadOnly +
            ", countUnread=" + countUnread +
            ", batchSize=" + batchSize +
            '}';
    }

}
