package com.jivesoftware.os.miru.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.os.miru.api.query.MiruTimeRange;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;

/**
 *
 */
public class MiruAggregateCountsQueryCriteria {

    private final Id streamId;
    private final MiruTimeRange answerTimeRange;
    private final MiruTimeRange countTimeRange;
    private final MiruFilter streamFilter;
    private final MiruFilter constraintsFilter;
    private final String query;
    private final String aggregateCountAroundField;
    private final int startFromDistinctN;
    private final int desiredNumberOfDistincts;

    private MiruAggregateCountsQueryCriteria(
        Id streamId,
        MiruTimeRange answerTimeRange,
        MiruTimeRange countTimeRange,
        MiruFilter streamFilter,
        MiruFilter constraintsFilter,
        String query,
        String aggregateCountAroundField,
        int startFromDistinctN,
        int desiredNumberOfDistincts) {
        this.streamId = streamId;
        this.answerTimeRange = answerTimeRange;
        this.countTimeRange = countTimeRange;
        this.streamFilter = streamFilter;
        this.constraintsFilter = constraintsFilter;
        this.query = query;
        this.aggregateCountAroundField = aggregateCountAroundField;
        this.startFromDistinctN = startFromDistinctN;
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    @JsonCreator
    public static MiruAggregateCountsQueryCriteria fromJson(
        @JsonProperty("streamId") Id streamId,
        @JsonProperty("answerTimeRange") MiruTimeRange answerTimeRange,
        @JsonProperty("countTimeRange") MiruTimeRange countTimeRange,
        @JsonProperty("streamFilter") MiruFilter streamFilter,
        @JsonProperty("contraintsFilter") MiruFilter constraintsFilter,
        @JsonProperty("query") String query,
        @JsonProperty("aggregateCountAroundField") String aggregateCountAroundField,
        @JsonProperty("startFromDistinctN") int startFromDistinctN,
        @JsonProperty("desiredNumberOfDistincts") int desiredNumberOfDistincts) {
        return new Builder()
            .setStreamId(streamId)
            .setAnswerTimeRange(answerTimeRange)
            .setCountTimeRange(countTimeRange)
            .setStreamFilter(streamFilter)
            .setConstraintsFilter(constraintsFilter)
            .setQuery(query)
            .setAggregateCountAroundField(aggregateCountAroundField)
            .setStartFromDistinctN(startFromDistinctN)
            .setDesiredNumberOfDistincts(desiredNumberOfDistincts)
            .build();
    }

    public Id getStreamId() {
        return streamId;
    }

    public MiruTimeRange getAnswerTimeRange() {
        return answerTimeRange;
    }

    public MiruTimeRange getCountTimeRange() {
        return countTimeRange;
    }

    public MiruFilter getStreamFilter() {
        return streamFilter;
    }

    public MiruFilter getConstraintsFilter() {
        return constraintsFilter;
    }

    public String getQuery() {
        return query;
    }

    public String getAggregateCountAroundField() {
        return aggregateCountAroundField;
    }

    public int getStartFromDistinctN() {
        return startFromDistinctN;
    }

    public int getDesiredNumberOfDistincts() {
        return desiredNumberOfDistincts;
    }

    @Override
    public String toString() {
        return "MiruAggregateCountsQueryCriteria{" +
            "streamId=" + streamId +
            ", answerTimeRange=" + answerTimeRange +
            ", countTimeRange=" + countTimeRange +
            ", streamFilter=" + streamFilter +
            ", constraintsFilter=" + constraintsFilter +
            ", query='" + query + '\'' +
            ", aggregateCountAroundField='" + aggregateCountAroundField + '\'' +
            ", startFromDistinctN=" + startFromDistinctN +
            ", desiredNumberOfDistincts=" + desiredNumberOfDistincts +
            '}';
    }

    public static final class Builder {

        private Id streamId; // optional
        private MiruTimeRange answerTimeRange; // optional
        private MiruTimeRange countTimeRange; // optional
        private MiruFilter streamFilter = MiruFilter.DEFAULT_FILTER;
        private MiruFilter constraintsFilter; // optional
        private String query = "";
        private String aggregateCountAroundField = MiruFieldName.ACTIVITY_PARENT.getFieldName();
        private int startFromDistinctN = 0;
        private int desiredNumberOfDistincts = 11;

        public Builder() {
        }

        public Builder setStreamId(Id streamId) {
            this.streamId = streamId;
            return this;
        }

        public Builder setAnswerTimeRange(MiruTimeRange answerTimeRange) {
            this.answerTimeRange = answerTimeRange;
            return this;
        }

        public Builder setCountTimeRange(MiruTimeRange countTimeRange) {
            this.countTimeRange = countTimeRange;
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

        public Builder setQuery(String query) {
            this.query = query;
            return this;
        }

        public Builder setAggregateCountAroundField(String aggregateCountAroundField) {
            this.aggregateCountAroundField = aggregateCountAroundField;
            return this;
        }

        public Builder setStartFromDistinctN(int startFromDistinctN) {
            this.startFromDistinctN = startFromDistinctN;
            return this;
        }

        public Builder setDesiredNumberOfDistincts(int desiredNumberOfDistincts) {
            this.desiredNumberOfDistincts = desiredNumberOfDistincts;
            return this;
        }

        public MiruAggregateCountsQueryCriteria build() {
            return new MiruAggregateCountsQueryCriteria(
                streamId,
                answerTimeRange,
                countTimeRange,
                streamFilter,
                constraintsFilter,
                query,
                aggregateCountAroundField,
                startFromDistinctN,
                desiredNumberOfDistincts);
        }
    }
}
