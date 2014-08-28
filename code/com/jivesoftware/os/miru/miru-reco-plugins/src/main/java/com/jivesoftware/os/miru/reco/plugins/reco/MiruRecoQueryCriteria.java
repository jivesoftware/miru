package com.jivesoftware.os.miru.reco.plugins.reco;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;

/**
 *
 */
public class MiruRecoQueryCriteria {

    private final MiruFilter constraintsFilter;
    private final MiruAuthzExpression authzExpression;
    private final String aggregateFieldName1;
    private final String retrieveFieldName1;
    private final String lookupFieldNamed1;
    private final String aggregateFieldName2;
    private final String retrieveFieldName2;
    private final String lookupFieldNamed2;
    private final String aggregateFieldName3;
    private final String retrieveFieldName3;
    private final int desiredNumberOfDistincts;

    public MiruRecoQueryCriteria(@JsonProperty("contraintsFilter") MiruFilter constraintsFilter,
            @JsonProperty("authzExpression") MiruAuthzExpression authzExpression,
            @JsonProperty("aggregateFieldName1") String aggregateFieldName1,
            @JsonProperty("retrieveFieldName1") String retrieveFieldName1,
            @JsonProperty("lookupFieldNamed1") String lookupFieldNamed1,
            @JsonProperty("aggregateFieldName2") String aggregateFieldName2,
            @JsonProperty("retrieveFieldName2") String retrieveFieldName2,
            @JsonProperty("lookupFieldNamed2") String lookupFieldNamed2,
            @JsonProperty("aggregateFieldName3") String aggregateFieldName3,
            @JsonProperty("retrieveFieldName3") String retrieveFieldName3,
            int desiredNumberOfDistincts) {
        this.constraintsFilter = constraintsFilter;
        this.authzExpression = authzExpression;
        this.aggregateFieldName1 = aggregateFieldName1;
        this.retrieveFieldName1 = retrieveFieldName1;
        this.lookupFieldNamed1 = lookupFieldNamed1;
        this.aggregateFieldName2 = aggregateFieldName2;
        this.retrieveFieldName2 = retrieveFieldName2;
        this.lookupFieldNamed2 = lookupFieldNamed2;
        this.aggregateFieldName3 = aggregateFieldName3;
        this.retrieveFieldName3 = retrieveFieldName3;
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    public MiruFilter getConstraintsFilter() {
        return constraintsFilter;
    }

    public MiruAuthzExpression getAuthzExpression() {
        return authzExpression;
    }

    public String getAggregateFieldName1() {
        return aggregateFieldName1;
    }

    public String getRetrieveFieldName1() {
        return retrieveFieldName1;
    }

    public String getLookupFieldNamed1() {
        return lookupFieldNamed1;
    }

    public String getAggregateFieldName2() {
        return aggregateFieldName2;
    }

    public String getRetrieveFieldName2() {
        return retrieveFieldName2;
    }

    public String getLookupFieldNamed2() {
        return lookupFieldNamed2;
    }

    public String getAggregateFieldName3() {
        return aggregateFieldName3;
    }

    public String getRetrieveFieldName3() {
        return retrieveFieldName3;
    }

    public int getDesiredNumberOfDistincts() {
        return desiredNumberOfDistincts;
    }

    @Override
    public String toString() {
        return "MiruRecoQueryCriteria{"
                + "constraintsFilter=" + constraintsFilter
                + ", authzExpression=" + authzExpression
                + ", aggregateFieldName1=" + aggregateFieldName1
                + ", retrieveFieldName1=" + retrieveFieldName1
                + ", lookupFieldNamed1=" + lookupFieldNamed1
                + ", aggregateFieldName2=" + aggregateFieldName2
                + ", retrieveFieldName2=" + retrieveFieldName2
                + ", lookupFieldNamed2=" + lookupFieldNamed2
                + ", aggregateFieldName3=" + aggregateFieldName3
                + ", retrieveFieldName3=" + retrieveFieldName3
                + ", desiredNumberOfDistincts=" + desiredNumberOfDistincts
                + '}';
    }

}
