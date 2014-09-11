package com.jivesoftware.os.miru.stream.plugins.filter;

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
public class AggregateCountsQuery {

    public final MiruTenantId tenantId;
    public final MiruStreamId streamId;
    public final MiruTimeRange answerTimeRange;
    public final MiruTimeRange countTimeRange;
    public final MiruFilter streamFilter;
    public final MiruFilter constraintsFilter;
    public final MiruAuthzExpression authzExpression;
    public final String aggregateCountAroundField;
    public final int startFromDistinctN;
    public final int desiredNumberOfDistincts;

    public AggregateCountsQuery(
            @JsonProperty("tenantId") MiruTenantId tenantId,
            @JsonProperty("streamId") MiruStreamId streamId,
            @JsonProperty("answerTimeRange") MiruTimeRange answerTimeRange,
            @JsonProperty("countTimeRange") MiruTimeRange countTimeRange,
            @JsonProperty("streamFilter") MiruFilter streamFilter,
            @JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
            @JsonProperty("authzExpression") MiruAuthzExpression authzExpression,
            @JsonProperty("aggregateCountAroundField") String aggregateCountAroundField,
            @JsonProperty("startFromDistinctN") int startFromDistinctN,
            @JsonProperty("desiredNumberOfDistincts") int desiredNumberOfDistincts) {
        this.tenantId = Preconditions.checkNotNull(tenantId);
        this.streamId = Preconditions.checkNotNull(streamId);
        this.answerTimeRange = Preconditions.checkNotNull(answerTimeRange);
        this.countTimeRange = Preconditions.checkNotNull(countTimeRange);
        this.streamFilter = Preconditions.checkNotNull(streamFilter);
        this.constraintsFilter = Preconditions.checkNotNull(constraintsFilter);
        this.authzExpression = Preconditions.checkNotNull(authzExpression);
        this.aggregateCountAroundField = Preconditions.checkNotNull(aggregateCountAroundField);
        Preconditions.checkArgument(startFromDistinctN >= 0, "Start from distinct must be at least 0");
        this.startFromDistinctN = startFromDistinctN;
        Preconditions.checkArgument(desiredNumberOfDistincts > 0, "Number of distincts must be at least 1");
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    @Override
    public String toString() {
        return "AggregateCountsQuery{" +
                "tenantId=" + tenantId +
                ", streamId=" + streamId +
                ", answerTimeRange=" + answerTimeRange +
                ", countTimeRange=" + countTimeRange +
                ", streamFilter=" + streamFilter +
                ", constraintsFilter=" + constraintsFilter +
                ", authzExpression=" + authzExpression +
                ", aggregateCountAroundField='" + aggregateCountAroundField + '\'' +
                ", startFromDistinctN=" + startFromDistinctN +
                ", desiredNumberOfDistincts=" + desiredNumberOfDistincts +
                '}';
    }
}
