package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.jivesoftware.os.miru.api.query.filter.MiruFilter;
import com.jivesoftware.os.miru.reco.plugins.distincts.DistinctsQuery;
import java.io.Serializable;
import java.util.List;

/**
 *
 */
public class TrendingQuery implements Serializable {

    public enum Strategy {

        LINEAR_REGRESSION, LEADER, PEAKS, HIGHEST_PEAK;
    }

    public final List<TrendingQueryScoreSet> scoreSets;
    public final MiruFilter constraintsFilter;
    public final String aggregateCountAroundField;
    public final List<List<DistinctsQuery>> distinctQueries; // inner lists are OR'd together, outer list is AND'd together

    @JsonCreator
    public TrendingQuery(
        @JsonProperty("scoreSets") List<TrendingQueryScoreSet> scoreSets,
        @JsonProperty("constraintsFilter") MiruFilter constraintsFilter,
        @JsonProperty("aggregateCountAroundField") String aggregateCountAroundField,
        @JsonProperty("distinctQueries") List<List<DistinctsQuery>> distinctQueries) {
        this.scoreSets = Preconditions.checkNotNull(scoreSets);
        this.constraintsFilter = Preconditions.checkNotNull(constraintsFilter);
        this.aggregateCountAroundField = Preconditions.checkNotNull(aggregateCountAroundField);
        this.distinctQueries = Preconditions.checkNotNull(distinctQueries);
    }

    @Override
    public String toString() {
        return "TrendingQuery{" +
            "scoreSets=" + scoreSets +
            ", constraintsFilter=" + constraintsFilter +
            ", aggregateCountAroundField='" + aggregateCountAroundField + '\'' +
            ", distinctQueries=" + distinctQueries +
            '}';
    }
}
