package com.jivesoftware.os.miru.stream.plugins.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Set;

/**
 * @author jonathan.colt
 */
public class AggregateCountsAnswerConstraint {

    public final List<AggregateCount> results;
    public final Set<String> aggregateTerms;
    public final int skippedDistincts;
    public final int collectedDistincts;

    @JsonCreator
    public AggregateCountsAnswerConstraint(
        @JsonProperty("results") List<AggregateCount> results,
        @JsonProperty("aggregateTerms") Set<String> aggregateTerms,
        @JsonProperty("skippedDistincts") int skippedDistincts,
        @JsonProperty("collectedDistincts") int collectedDistincts) {
        this.results = results;
        this.aggregateTerms = aggregateTerms;
        this.skippedDistincts = skippedDistincts;
        this.collectedDistincts = collectedDistincts;
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
