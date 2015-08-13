package com.jivesoftware.os.miru.reco.plugins.reco;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;

/**
 *
 */
public class RecoQuery {

    public final MiruTimeRange timeRange;
    public final DistinctsQuery removeDistinctsQuery;
    public final MiruFilter constraintsFilter;
    public final String aggregateFieldName1;
    public final String retrieveFieldName1;
    public final String lookupFieldName1;
    public final String aggregateFieldName2;
    public final String retrieveFieldName2;
    public final String lookupFieldName2;
    public final String aggregateFieldName3;
    public final String retrieveFieldName3;
    public final MiruFilter scorableFilter;
    public final int desiredNumberOfDistincts;

    public RecoQuery(@JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("removeDistinctsQuery") DistinctsQuery removeDistinctsQuery,
        @JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
        @JsonProperty("aggregateFieldName1") String aggregateFieldName1,
        @JsonProperty("retrieveFieldName1") String retrieveFieldName1,
        @JsonProperty("lookupFieldName1") String lookupFieldName1,
        @JsonProperty("aggregateFieldName2") String aggregateFieldName2,
        @JsonProperty("retrieveFieldName2") String retrieveFieldName2,
        @JsonProperty("lookupFieldName2") String lookupFieldName2,
        @JsonProperty("aggregateFieldName3") String aggregateFieldName3,
        @JsonProperty("retrieveFieldName3") String retrieveFieldName3,
        @JsonProperty("scorableFilter") MiruFilter scorableFilter,
        @JsonProperty("desiredNumberOfDistincts") int desiredNumberOfDistincts) {
        Preconditions.checkArgument(!MiruTimeRange.ALL_TIME.equals(timeRange), "Requires an explicit time range");
        this.timeRange = Preconditions.checkNotNull(timeRange);
        this.removeDistinctsQuery = removeDistinctsQuery;
        this.constraintsFilter = Preconditions.checkNotNull(constraintsFilter);
        this.aggregateFieldName1 = Preconditions.checkNotNull(aggregateFieldName1);
        this.retrieveFieldName1 = Preconditions.checkNotNull(retrieveFieldName1);
        this.lookupFieldName1 = Preconditions.checkNotNull(lookupFieldName1);
        this.aggregateFieldName2 = Preconditions.checkNotNull(aggregateFieldName2);
        this.retrieveFieldName2 = Preconditions.checkNotNull(retrieveFieldName2);
        this.lookupFieldName2 = Preconditions.checkNotNull(lookupFieldName2);
        this.aggregateFieldName3 = Preconditions.checkNotNull(aggregateFieldName3);
        this.retrieveFieldName3 = Preconditions.checkNotNull(retrieveFieldName3);
        this.scorableFilter = scorableFilter;
        Preconditions.checkArgument(desiredNumberOfDistincts > 0, "Number of distincts must be at least 1");
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    @Override
    public String toString() {
        return "RecoQuery{" +
            "removeDistinctsQuery=" + removeDistinctsQuery +
            ", constraintsFilter=" + constraintsFilter +
            ", aggregateFieldName1='" + aggregateFieldName1 + '\'' +
            ", retrieveFieldName1='" + retrieveFieldName1 + '\'' +
            ", lookupFieldName1='" + lookupFieldName1 + '\'' +
            ", aggregateFieldName2='" + aggregateFieldName2 + '\'' +
            ", retrieveFieldName2='" + retrieveFieldName2 + '\'' +
            ", lookupFieldName2='" + lookupFieldName2 + '\'' +
            ", aggregateFieldName3='" + aggregateFieldName3 + '\'' +
            ", retrieveFieldName3='" + retrieveFieldName3 + '\'' +
            ", scorableFilter=" + scorableFilter +
            ", desiredNumberOfDistincts=" + desiredNumberOfDistincts +
            '}';
    }

}
