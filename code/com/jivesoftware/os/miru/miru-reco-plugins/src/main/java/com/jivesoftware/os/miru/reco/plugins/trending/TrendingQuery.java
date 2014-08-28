package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;

/**
 *
 */
public class TrendingQuery {

    public final MiruTenantId tenantId;
    public final Optional<MiruAuthzExpression> authzExpression;
    public final MiruFilter constraintsFilter;
    public final String aggregateCountAroundField;
    public final int desiredNumberOfDistincts;

    public TrendingQuery(
        MiruTenantId tenantId,
        Optional<MiruAuthzExpression> authzExpression,
        MiruFilter constraintsFilter,
        String aggregateCountAroundField,
        int desiredNumberOfDistincts) {
        this.tenantId = tenantId;
        this.authzExpression = authzExpression;
        this.constraintsFilter = constraintsFilter;
        this.aggregateCountAroundField = aggregateCountAroundField;
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    @JsonCreator
    public static TrendingQuery fromJson(
        @JsonProperty("tenantId") MiruTenantId tenantId,
        @JsonProperty("authzExpression") Optional<MiruAuthzExpression> authzExpression,
        @JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
        @JsonProperty("aggregateCountAroundField") String trendingAroundField,
        @JsonProperty("desiredNumberOfDistincts") int desiredNumberOfTrending) {
        return new TrendingQuery(tenantId, authzExpression, constraintsFilter, trendingAroundField, desiredNumberOfTrending);
    }

    @Override
    public String toString() {
        return "TrendingQuery{" +
            "tenantId=" + tenantId +
            ", authzExpression=" + authzExpression +
            ", constraintsFilter=" + constraintsFilter +
            ", aggregateCountAroundField='" + aggregateCountAroundField + '\'' +
            ", desiredNumberOfDistincts=" + desiredNumberOfDistincts +
            '}';
    }
}
