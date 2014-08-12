package com.jivesoftware.os.miru.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;

/**
 *
 */
public class MiruTrendingQueryCriteria {

    private final MiruFilter constraintsFilter;
    private final String aggregateCountAroundField;
    private final int desiredNumberOfDistincts;

    private MiruTrendingQueryCriteria(
        MiruFilter constraintsFilter,
        String aggregateCountAroundField,
        int desiredNumberOfDistincts) {
        this.constraintsFilter = constraintsFilter;
        this.aggregateCountAroundField = aggregateCountAroundField;
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    @JsonCreator
    public static MiruTrendingQueryCriteria fromJson(
        @JsonProperty("contraintsFilter") MiruFilter constraintsFilter,
        @JsonProperty("aggregateCountAroundField") String aggregateCountAroundField,
        @JsonProperty("desiredNumberOfDistincts") int desiredNumberOfDistincts) {
        return new Builder()
            .setConstraintsFilter(constraintsFilter)
            .setAggregateCountAroundField(aggregateCountAroundField)
            .setDesiredNumberOfDistincts(desiredNumberOfDistincts)
            .build();
    }

    public MiruFilter getConstraintsFilter() {
        return constraintsFilter;
    }

    public String getAggregateCountAroundField() {
        return aggregateCountAroundField;
    }

    public int getDesiredNumberOfDistincts() {
        return desiredNumberOfDistincts;
    }

    @Override
    public String toString() {
        return "MiruTrendingQueryCriteria{" +
            "constraintsFilter=" + constraintsFilter +
            ", aggregateCountAroundField='" + aggregateCountAroundField + '\'' +
            ", desiredNumberOfDistincts=" + desiredNumberOfDistincts +
            '}';
    }

    public static final class Builder {

        private MiruFilter constraintsFilter = MiruFilter.DEFAULT_FILTER;
        private String aggregateCountAroundField = MiruFieldName.ACTIVITY_PARENT.getFieldName();
        private int desiredNumberOfDistincts = 51;

        public Builder() {
        }

        public Builder setConstraintsFilter(MiruFilter constraintsFilter) {
            this.constraintsFilter = constraintsFilter;
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

        public MiruTrendingQueryCriteria build() {
            return new MiruTrendingQueryCriteria(
                constraintsFilter,
                aggregateCountAroundField,
                desiredNumberOfDistincts);
        }
    }
}
