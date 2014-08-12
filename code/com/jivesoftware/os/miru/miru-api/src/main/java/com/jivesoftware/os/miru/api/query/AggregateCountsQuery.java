package com.jivesoftware.os.miru.api.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;

/** @author jonathan */
public class AggregateCountsQuery {

    public final MiruTenantId tenantId;
    public final Optional<MiruStreamId> streamId;
    public final Optional<MiruTimeRange> answerTimeRange;
    public final Optional<MiruTimeRange> countTimeRange;
    public final Optional<MiruAuthzExpression> authzExpression;
    public final MiruFilter streamFilter;
    public final Optional<MiruFilter> constraintsFilter;
    public final String query;
    public final String aggregateCountAroundField; // implied that we are counting distict id which is the value part of ActivityTypedalue
    public final int startFromDistinctN;
    public final int desiredNumberOfDistincts;

    public AggregateCountsQuery(
        MiruTenantId tenantId,
        Optional<MiruStreamId> streamId,
        Optional<MiruTimeRange> answerTimeRange,
        Optional<MiruTimeRange> countTimeRange,
        Optional<MiruAuthzExpression> authzExpression,
        MiruFilter streamFilter,
        Optional<MiruFilter> constraintsFilter,
        String query,
        String aggregateCountAroundField,
        int startFromDistinctN,
        int desiredNumberOfDistincts) {
        this.tenantId = tenantId;
        this.streamId = streamId;
        this.answerTimeRange = answerTimeRange;
        this.countTimeRange = countTimeRange;
        this.authzExpression = authzExpression;
        this.streamFilter = streamFilter;
        this.constraintsFilter = constraintsFilter;
        this.query = query;
        this.aggregateCountAroundField = aggregateCountAroundField;
        this.startFromDistinctN = startFromDistinctN;
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    @JsonCreator
    public static AggregateCountsQuery fromJson(
        @JsonProperty("tenantId") MiruTenantId tenantId,
        @JsonProperty("streamId") Optional<MiruStreamId> streamId,
        @JsonProperty("answerTimeRange") Optional<MiruTimeRange> answerTimeRange,
        @JsonProperty("countTimeRange") Optional<MiruTimeRange> countTimeRange,
        @JsonProperty("authzExpression") Optional<MiruAuthzExpression> authzExpression,
        @JsonProperty("streamFilter") MiruFilter streamFilter,
        @JsonProperty("constraintsFilter") Optional<MiruFilter> constraintsFilter,
        @JsonProperty("query") String query,
        @JsonProperty("aggregateCountAroundField") String aggregateCountAroundField,
        @JsonProperty("startFromDistinctN") int startFromDistinctN,
        @JsonProperty("desiredNumberOfDistincts") int desiredNumberOfDistincts) {
        return new AggregateCountsQuery(tenantId, streamId, answerTimeRange, countTimeRange, authzExpression, streamFilter, constraintsFilter,
            query, aggregateCountAroundField, startFromDistinctN, desiredNumberOfDistincts);
    }

    @Override
    public String toString() {
        return "AggregateCountsQuery{" +
            "tenantId=" + tenantId +
            ", streamId=" + streamId +
            ", answerTimeRange=" + answerTimeRange +
            ", countTimeRange=" + countTimeRange +
            ", authzExpression=" + authzExpression +
            ", streamFilter=" + streamFilter +
            ", constraintsFilter=" + constraintsFilter +
            ", query='" + query + '\'' +
            ", aggregateCountAroundField='" + aggregateCountAroundField + '\'' +
            ", startFromDistinctN=" + startFromDistinctN +
            ", desiredNumberOfDistincts=" + desiredNumberOfDistincts +
            '}';
    }

}
