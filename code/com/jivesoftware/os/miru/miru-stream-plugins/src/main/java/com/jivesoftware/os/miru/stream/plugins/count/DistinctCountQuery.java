package com.jivesoftware.os.miru.stream.plugins.count;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.query.MiruTimeRange;

/**
 *
 */
public class DistinctCountQuery {

    public final MiruTenantId tenantId;
    public final MiruStreamId streamId;
    public final MiruTimeRange timeRange;
    public final MiruFilter streamFilter;
    public final MiruFilter constraintsFilter;
    public final MiruAuthzExpression authzExpression;
    public final String aggregateCountAroundField;
    public final int desiredNumberOfDistincts;

    public DistinctCountQuery(
            @JsonProperty("tenantId") MiruTenantId tenantId,
            @JsonProperty("streamId") MiruStreamId streamId,
            @JsonProperty("timeRange") MiruTimeRange timeRange,
            @JsonProperty("streamFilter") MiruFilter streamFilter,
            @JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
            @JsonProperty("authzExpression") MiruAuthzExpression authzExpression,
            @JsonProperty("aggregateCountAroundField") String aggregateCountAroundField,
            @JsonProperty("desiredNumberOfDistincts") int desiredNumberOfDistincts) {
        this.tenantId = Preconditions.checkNotNull(tenantId);
        this.streamId = Preconditions.checkNotNull(streamId);
        this.timeRange = Preconditions.checkNotNull(timeRange);
        this.streamFilter = Preconditions.checkNotNull(streamFilter);
        this.constraintsFilter = Preconditions.checkNotNull(constraintsFilter);
        this.authzExpression = Preconditions.checkNotNull(authzExpression);
        this.aggregateCountAroundField = Preconditions.checkNotNull(aggregateCountAroundField);
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    @Override
    public String toString() {
        return "MiruDistinctCountQueryCriteria{" +
                "tenantId=" + tenantId +
                ", streamId=" + streamId +
                ", timeRange=" + timeRange +
                ", streamFilter=" + streamFilter +
                ", constraintsFilter=" + constraintsFilter +
                ", authzExpression=" + authzExpression +
                ", aggregateCountAroundField='" + aggregateCountAroundField + '\'' +
                ", desiredNumberOfDistincts=" + desiredNumberOfDistincts +
                '}';
    }
}
