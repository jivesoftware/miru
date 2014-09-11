package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.query.MiruTimeRange;

/**
 *
 */
public class TrendingQuery {

    public final MiruTenantId tenantId;
    public final MiruTimeRange timeRange;
    public final int divideTimeRangeIntoNSegments;
    public final MiruFilter constraintsFilter;
    public final MiruAuthzExpression authzExpression;
    public final String aggregateCountAroundField;
    public final int desiredNumberOfDistincts;

    @JsonCreator
    public TrendingQuery(
            @JsonProperty("tenantId") MiruTenantId tenantId,
            @JsonProperty("timeRange") MiruTimeRange timeRange,
            @JsonProperty("divideTimeRangeIntoNSegments") int divideTimeRangeIntoNSegments,
            @JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
            @JsonProperty("authzExpression") MiruAuthzExpression authzExpression,
            @JsonProperty("aggregateCountAroundField") String aggregateCountAroundField,
            @JsonProperty("desiredNumberOfDistincts") int desiredNumberOfDistincts) {
        this.tenantId = Preconditions.checkNotNull(tenantId);
        Preconditions.checkArgument(!MiruTimeRange.ALL_TIME.equals(timeRange), "Requires an explicit time range");
        this.timeRange = Preconditions.checkNotNull(timeRange);
        Preconditions.checkArgument(divideTimeRangeIntoNSegments > 0, "Segments must be at least 1");
        this.divideTimeRangeIntoNSegments = divideTimeRangeIntoNSegments;
        this.constraintsFilter = Preconditions.checkNotNull(constraintsFilter);
        this.authzExpression = Preconditions.checkNotNull(authzExpression);
        this.aggregateCountAroundField = Preconditions.checkNotNull(aggregateCountAroundField);
        Preconditions.checkArgument(desiredNumberOfDistincts > 0, "Number of distincts must be at least 1");
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    @Override
    public String toString() {
        return "TrendingQuery{" +
                "tenantId=" + tenantId +
                ", timeRange=" + timeRange +
                ", divideTimeRangeIntoNSegments=" + divideTimeRangeIntoNSegments +
                ", constraintsFilter=" + constraintsFilter +
                ", authzExpression=" + authzExpression +
                ", aggregateCountAroundField='" + aggregateCountAroundField + '\'' +
                ", desiredNumberOfDistincts=" + desiredNumberOfDistincts +
                '}';
    }
}
