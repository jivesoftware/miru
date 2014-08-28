package com.jivesoftware.os.miru.reco.plugins.reco;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;

/**
 *
 */
public class RecoQuery {

    public final MiruTenantId tenantId;
    public final Optional<MiruAuthzExpression> authzExpression;
    public final MiruFilter constraintsFilter;
    public final String aggregateFieldName1;
    public final String retrieveFieldName1;
    public final String lookupFieldNamed1;
    public final String aggregateFieldName2;
    public final String retrieveFieldName2;
    public final String lookupFieldNamed2;
    public final String aggregateFieldName3;
    public final String retrieveFieldName3;

    public final int resultCount;

    public RecoQuery(@JsonProperty("tenantId") MiruTenantId tenantId,
        @JsonProperty("authzExpression") Optional<MiruAuthzExpression> authzExpression,
        @JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
        @JsonProperty("aggregateFieldName1") String aggregateFieldName1,
        @JsonProperty("retrieveFieldName1") String retrieveFieldName1,
        @JsonProperty("lookupFieldNamed1") String lookupFieldNamed1,
        @JsonProperty("aggregateFieldName2") String aggregateFieldName2,
        @JsonProperty("retrieveFieldName2") String retrieveFieldName2,
        @JsonProperty("lookupFieldNamed2") String lookupFieldNamed2,
        @JsonProperty("aggregateFieldName3") String aggregateFieldName3,
        @JsonProperty("retrieveFieldName3") String retrieveFieldName3,
        @JsonProperty("resultCount") int resultCount) {
        this.tenantId = tenantId;
        this.authzExpression = authzExpression;
        this.constraintsFilter = constraintsFilter;
        this.aggregateFieldName1 = aggregateFieldName1;
        this.retrieveFieldName1 = retrieveFieldName1;
        this.lookupFieldNamed1 = lookupFieldNamed1;
        this.aggregateFieldName2 = aggregateFieldName2;
        this.retrieveFieldName2 = retrieveFieldName2;
        this.lookupFieldNamed2 = lookupFieldNamed2;
        this.aggregateFieldName3 = aggregateFieldName3;
        this.retrieveFieldName3 = retrieveFieldName3;
        this.resultCount = resultCount;
    }

    @Override
    public String toString() {
        return "RecoQuery{"
                + "tenantId=" + tenantId
                + ", authzExpression=" + authzExpression
                + ", constraintsFilter=" + constraintsFilter
                + ", aggregateFieldName1=" + aggregateFieldName1
                + ", retrieveFieldName1=" + retrieveFieldName1
                + ", lookupFieldNamed1=" + lookupFieldNamed1
                + ", aggregateFieldName2=" + aggregateFieldName2
                + ", retrieveFieldName2=" + retrieveFieldName2
                + ", lookupFieldNamed2=" + lookupFieldNamed2
                + ", aggregateFieldName3=" + aggregateFieldName3
                + ", retrieveFieldName3=" + retrieveFieldName3
                + ", resultCount=" + resultCount
                + '}';
    }


}
