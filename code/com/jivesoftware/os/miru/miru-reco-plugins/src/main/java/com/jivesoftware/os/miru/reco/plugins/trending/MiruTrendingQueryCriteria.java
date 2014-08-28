package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.field.MiruFieldName;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;

/**
 *
 */
public class MiruTrendingQueryCriteria {

    private final MiruFilter constraintsFilter;
    private final MiruAuthzExpression authzExpression;
    private final String aggregateCountAroundField;
    private final int desiredNumberOfDistincts;

    private MiruTrendingQueryCriteria(
            MiruFilter constraintsFilter,
            MiruAuthzExpression authzExpression,
            String aggregateCountAroundField,
            int desiredNumberOfDistincts) {
        this.constraintsFilter = constraintsFilter;
        this.authzExpression = authzExpression;
        this.aggregateCountAroundField = aggregateCountAroundField;
        this.desiredNumberOfDistincts = desiredNumberOfDistincts;
    }

    @JsonCreator
    public static MiruTrendingQueryCriteria fromJson(
            @JsonProperty("contraintsFilter") MiruFilter constraintsFilter,
            @JsonProperty("authzExpression") MiruAuthzExpression authzExpression,
            @JsonProperty("aggregateCountAroundField") String aggregateCountAroundField,
            @JsonProperty("desiredNumberOfDistincts") int desiredNumberOfDistincts) {
        return new Builder()
                .setConstraintsFilter(constraintsFilter)
                .setAuthzExpression(authzExpression)
                .setAggregateCountAroundField(aggregateCountAroundField)
                .setDesiredNumberOfDistincts(desiredNumberOfDistincts)
                .build();
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
        return "MiruTrendingQueryCriteria{" +
                "constraintsFilter=" + constraintsFilter +
                ", authzExpression=" + authzExpression +
                ", aggregateCountAroundField='" + aggregateCountAroundField + '\'' +
                ", desiredNumberOfDistincts=" + desiredNumberOfDistincts +
                '}';
    }

    public static final class Builder {

        private MiruFilter constraintsFilter = MiruFilter.DEFAULT_FILTER;
        private MiruAuthzExpression authzExpression;
        private String aggregateCountAroundField = MiruFieldName.ACTIVITY_PARENT.getFieldName();
        private int desiredNumberOfDistincts = 51;

        public Builder() {
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

        public MiruTrendingQueryCriteria build() {
            return new MiruTrendingQueryCriteria(
                    constraintsFilter,
                    authzExpression,
                    aggregateCountAroundField,
                    desiredNumberOfDistincts);
        }
    }
}
