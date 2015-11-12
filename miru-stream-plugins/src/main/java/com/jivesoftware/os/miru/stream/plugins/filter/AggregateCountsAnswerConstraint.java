package com.jivesoftware.os.miru.stream.plugins.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class AggregateCountsAnswerConstraint {

    public final ImmutableList<AggregateCount> results;
    public final ImmutableSet<String> aggregateTerms;
    public final int skippedDistincts;
    public final int collectedDistincts;

    public AggregateCountsAnswerConstraint(ImmutableList<AggregateCount> results, ImmutableSet<String> aggregateTerms, int skippedDistincts,
        int collectedDistincts) {
        this.results = results;
        this.aggregateTerms = aggregateTerms;
        this.skippedDistincts = skippedDistincts;
        this.collectedDistincts = collectedDistincts;
    }

    @JsonCreator
    public static AggregateCountsAnswerConstraint fromJson(
        @JsonProperty("results") List<AggregateCount> results,
        @JsonProperty("aggregateTerms") Set<String> aggregateTerms,
        @JsonProperty("skippedDistincts") int skippedDistincts,
        @JsonProperty("collectedDistincts") int collectedDistincts) {
        return new AggregateCountsAnswerConstraint(ImmutableList.copyOf(results),
            ImmutableSet.copyOf(aggregateTerms),
            skippedDistincts,
            collectedDistincts);
    }

    @Override
    public String toString() {
        return "AggregateCountsAnswerConstraint{"
            + "results=" + results
            + ", aggregateTerms=" + aggregateTerms
            + ", skippedDistincts=" + skippedDistincts
            + ", collectedDistincts=" + collectedDistincts
            + '}';
    }

}
