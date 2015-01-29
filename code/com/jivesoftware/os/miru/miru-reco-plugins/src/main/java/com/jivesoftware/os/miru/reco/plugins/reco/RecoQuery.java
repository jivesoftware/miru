package com.jivesoftware.os.miru.reco.plugins.reco;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;

/**
 *
 */
public class RecoQuery {

    public final MiruFilter constraintsFilter;
    public final String aggregateFieldName1;
    public final String retrieveFieldName1;
    public final String lookupFieldNamed1;
    public final String aggregateFieldName2;
    public final String retrieveFieldName2;
    public final String lookupFieldNamed2;
    public final String aggregateFieldName3;
    public final String retrieveFieldName3;
    public final MiruFilter scorableFilter;
    public final int desiredNumberOfDistincts;

    public RecoQuery(@JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
            @JsonProperty("aggregateFieldName1") String aggregateFieldName1,
            @JsonProperty("retrieveFieldName1") String retrieveFieldName1,
            @JsonProperty("lookupFieldNamed1") String lookupFieldNamed1,
            @JsonProperty("aggregateFieldName2") String aggregateFieldName2,
            @JsonProperty("retrieveFieldName2") String retrieveFieldName2,
            @JsonProperty("lookupFieldNamed2") String lookupFieldNamed2,
            @JsonProperty("aggregateFieldName3") String aggregateFieldName3,
            @JsonProperty("retrieveFieldName3") String retrieveFieldName3,
            @JsonProperty("scorableFilter") MiruFilter scorableFilter,
            @JsonProperty("desiredNumberOfDistincts") int desiredNumberOfDistincts) {
        this.constraintsFilter = Preconditions.checkNotNull(constraintsFilter);
        this.aggregateFieldName1 = Preconditions.checkNotNull(aggregateFieldName1);
        this.retrieveFieldName1 = Preconditions.checkNotNull(retrieveFieldName1);
        this.lookupFieldNamed1 = Preconditions.checkNotNull(lookupFieldNamed1);
        this.aggregateFieldName2 = Preconditions.checkNotNull(aggregateFieldName2);
        this.retrieveFieldName2 = Preconditions.checkNotNull(retrieveFieldName2);
        this.lookupFieldNamed2 = Preconditions.checkNotNull(lookupFieldNamed2);
        this.aggregateFieldName3 = Preconditions.checkNotNull(aggregateFieldName3);
        this.retrieveFieldName3 = Preconditions.checkNotNull(retrieveFieldName3);
        Preconditions.checkArgument(desiredNumberOfDistincts > 0, "Number of distincts must be at least 1");
        this.scorableFilter = scorableFilter;
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    @Override
    public String toString() {
        return "RecoQuery{" +
                "constraintsFilter=" + constraintsFilter +
                ", aggregateFieldName1='" + aggregateFieldName1 + '\'' +
                ", retrieveFieldName1='" + retrieveFieldName1 + '\'' +
                ", lookupFieldNamed1='" + lookupFieldNamed1 + '\'' +
                ", aggregateFieldName2='" + aggregateFieldName2 + '\'' +
                ", retrieveFieldName2='" + retrieveFieldName2 + '\'' +
                ", lookupFieldNamed2='" + lookupFieldNamed2 + '\'' +
                ", aggregateFieldName3='" + aggregateFieldName3 + '\'' +
                ", retrieveFieldName3='" + retrieveFieldName3 + '\'' +
                ", scorableFilter='" + scorableFilter + '\'' +
                ", desiredNumberOfDistincts=" + desiredNumberOfDistincts +
                '}';
    }

}
