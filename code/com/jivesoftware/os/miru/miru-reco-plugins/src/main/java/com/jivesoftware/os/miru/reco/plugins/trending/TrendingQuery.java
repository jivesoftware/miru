package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;

/**
 *
 */
public class TrendingQuery {

    public final MiruTenantId tenantId;
    public final MiruFilter constraintsFilter;
    public final MiruAuthzExpression authzExpression;
    public final String aggregateCountAroundField;
    public final int desiredNumberOfDistincts;

    @JsonCreator
    public TrendingQuery(
            @JsonProperty("tenantId") MiruTenantId tenantId,
            @JsonProperty("contraintsFilter") MiruFilter constraintsFilter,
            @JsonProperty("authzExpression") MiruAuthzExpression authzExpression,
            @JsonProperty("aggregateCountAroundField") String aggregateCountAroundField,
            @JsonProperty("desiredNumberOfDistincts") int desiredNumberOfDistincts) {
        this.tenantId = Preconditions.checkNotNull(tenantId);
        this.constraintsFilter = Preconditions.checkNotNull(constraintsFilter);
        this.authzExpression = Preconditions.checkNotNull(authzExpression);
        this.aggregateCountAroundField = Preconditions.checkNotNull(aggregateCountAroundField);
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    @Override
    public String toString() {
        return "MiruTrendingQueryCriteria{" +
                "tenantId=" + tenantId +
                ", constraintsFilter=" + constraintsFilter +
                ", authzExpression=" + authzExpression +
                ", aggregateCountAroundField='" + aggregateCountAroundField + '\'' +
                ", desiredNumberOfDistincts=" + desiredNumberOfDistincts +
                '}';
    }
}
