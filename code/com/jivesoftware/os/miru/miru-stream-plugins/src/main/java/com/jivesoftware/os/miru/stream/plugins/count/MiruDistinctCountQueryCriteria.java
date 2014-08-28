package com.jivesoftware.os.miru.stream.plugins.count;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.query.MiruTimeRange;

/**
 *
 */
public class MiruDistinctCountQueryCriteria {

    private final Id streamId;
    private final MiruTimeRange timeRange;
    private final MiruFilter streamFilter;
    private final MiruFilter constraintsFilter;
    private final MiruAuthzExpression authzExpression;
    private final String aggregateCountAroundField;
    private final int desiredNumberOfDistincts;

    private MiruDistinctCountQueryCriteria(
            Id streamId,
            MiruTimeRange timeRange,
            MiruFilter streamFilter,
            MiruFilter constraintsFilter,
            MiruAuthzExpression authzExpression,
            String aggregateCountAroundField,
            int desiredNumberOfDistincts) {
        this.streamId = streamId;
        this.timeRange = timeRange;
        this.streamFilter = streamFilter;
        this.constraintsFilter = constraintsFilter;
        this.authzExpression = authzExpression;
        this.aggregateCountAroundField = aggregateCountAroundField;
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    @JsonCreator
    public static MiruDistinctCountQueryCriteria fromJson(
            @JsonProperty("streamId") Id streamId,
            @JsonProperty("timeRange") MiruTimeRange timeRange,
            @JsonProperty("streamFilter") MiruFilter streamFilter,
            @JsonProperty("contraintsFilter") MiruFilter constraintsFilter,
            @JsonProperty("authzExpression") MiruAuthzExpression authzExpression,
            @JsonProperty("aggregateCountAroundField") String aggregateCountAroundField,
            @JsonProperty("desiredNumberOfDistincts") int desiredNumberOfDistincts) {
        return new Builder()
                .setStreamId(streamId)
                .setTimeRange(timeRange)
                .setStreamFilter(streamFilter)
                .setConstraintsFilter(constraintsFilter)
                .setAuthzExpression(authzExpression)
                .setAggregateCountAroundField(aggregateCountAroundField)
                .setDesiredNumberOfDistincts(desiredNumberOfDistincts)
                .build();
    }

    public Id getStreamId() {
        return streamId;
    }

    public MiruTimeRange getTimeRange() {
        return timeRange;
    }

    public MiruFilter getStreamFilter() {
        return streamFilter;
    }

    public MiruFilter getConstraintsFilter() {
        return constraintsFilter;
    }

    public MiruAuthzExpression getAuthzExpression() {
        return authzExpression;
    }

    public String getAggregateCountAroundField() {
        return aggregateCountAroundField;
    }

    public int getDesiredNumberOfDistincts() {
        return desiredNumberOfDistincts;
    }

    @Override
    public String toString() {
        return "MiruQueryCriteria{" +
            "streamId=" + streamId +
            ", timeRange=" + timeRange +
            ", streamFilter=" + streamFilter +
            ", constraintsFilter=" + constraintsFilter +
            ", authzExpression=" + authzExpression +
            ", aggregateCountAroundField='" + aggregateCountAroundField + '\'' +
            ", desiredNumberOfDistincts=" + desiredNumberOfDistincts +
            '}';
    }

    public static final class Builder {

        private Id streamId; // optional
        private MiruTimeRange timeRange; // optional
        private MiruFilter streamFilter = MiruFilter.DEFAULT_FILTER;
        private MiruFilter constraintsFilter; // optional
        private MiruAuthzExpression authzExpression; // optional
        private String aggregateCountAroundField = MiruFieldName.ACTIVITY_PARENT.getFieldName();
        private int desiredNumberOfDistincts = 51;

        public Builder() {
        }

        public Builder setStreamId(Id streamId) {
            this.streamId = streamId;
            return this;
        }

        public Builder setTimeRange(MiruTimeRange timeRange) {
            this.timeRange = timeRange;
            return this;
        }

        public Builder setStreamFilter(MiruFilter streamFilter) {
            this.streamFilter = streamFilter;
            return this;
        }

        public Builder setConstraintsFilter(MiruFilter constraintsFilter) {
            this.constraintsFilter = constraintsFilter;
            return this;
        }

        public Builder setAuthzExpression(MiruAuthzExpression authzExpression) {
            this.authzExpression = authzExpression;
            return this;
        }

        public Builder setAggregateCountAroundField(String aggregateCountAroundField) {
            this.aggregateCountAroundField = aggregateCountAroundField;
            return this;
        }

        public Builder setDesiredNumberOfDistincts(int desiredNumberOfDistincts) {
            this.desiredNumberOfDistincts = desiredNumberOfDistincts;
            return this;
        }

        public MiruDistinctCountQueryCriteria build() {
            return new MiruDistinctCountQueryCriteria(
                streamId,
                timeRange,
                streamFilter,
                constraintsFilter,
                    authzExpression, aggregateCountAroundField,
                desiredNumberOfDistincts);
        }
    }
}
