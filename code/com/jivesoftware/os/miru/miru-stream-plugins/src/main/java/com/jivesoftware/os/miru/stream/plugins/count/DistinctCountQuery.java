package com.jivesoftware.os.miru.stream.plugins.count;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.query.MiruTimeRange;

/** @author jonathan */
public class DistinctCountQuery {

    public final MiruTenantId tenantId;
    public final Optional<MiruStreamId> streamId;
    public final Optional<MiruTimeRange> timeRange; // orderIds
    public final Optional<MiruAuthzExpression> authzExpression;
    public final MiruFilter streamFilter;
    public final Optional<MiruFilter> constraintsFilter;
    public final String aggregateCountAroundField;
    public final int desiredNumberOfDistincts;

    public DistinctCountQuery(MiruTenantId tenantId,
        Optional<MiruStreamId> streamId,
        Optional<MiruTimeRange> timeRange,
        Optional<MiruAuthzExpression> authzExpression,
        MiruFilter streamFilter,
        Optional<MiruFilter> constraintsFilter,
        String aggregateCountAroundField,
        int desiredNumberOfDistincts) {
        this.tenantId = tenantId;
        this.streamId = streamId;
        this.timeRange = timeRange;
        this.streamFilter = streamFilter;
        this.constraintsFilter = constraintsFilter;
        this.authzExpression = authzExpression;
        this.aggregateCountAroundField = aggregateCountAroundField;
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    @JsonCreator
    public static DistinctCountQuery fromJson(
        @JsonProperty("tenantId") MiruTenantId tenantId,
        @JsonProperty("streamId") Optional<MiruStreamId> streamId,
        @JsonProperty("timeRange") Optional<MiruTimeRange> timeRange,
        @JsonProperty("authzExpression") Optional<MiruAuthzExpression> authzExpression,
        @JsonProperty("streamFilter") MiruFilter streamFilter,
        @JsonProperty("constraintsFilter") Optional<MiruFilter> constraintsFilter,
        @JsonProperty("aggregateCountAroundField") String aggregateCountAroundField,
        @JsonProperty("desiredNumberOfDistincts") int desiredNumberOfDistincts) {

        return new DistinctCountQuery(tenantId, streamId, timeRange, authzExpression, streamFilter, constraintsFilter, aggregateCountAroundField,
            desiredNumberOfDistincts);
    }

    @Override
    public String toString() {
        return "DistinctCountQuery{" +
            "tenantId=" + tenantId +
            ", streamId=" + streamId +
            ", timeRange=" + timeRange +
            ", authzExpression=" + authzExpression +
            ", streamFilter=" + streamFilter +
            ", constraintsFilter=" + constraintsFilter +
            ", aggregateCountAroundField='" + aggregateCountAroundField + '\'' +
            ", desiredNumberOfDistincts=" + desiredNumberOfDistincts +
            '}';
    }

}
