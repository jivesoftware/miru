package com.jivesoftware.os.miru.stream.plugins.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import java.util.List;
import java.util.Set;

/**
 * @author jonathan.colt
 */
public class AggregateCountsAnswerConstraint {

    public final List<AggregateCount> results;
    public final Set<MiruValue> aggregateTerms;
    public final Set<MiruValue> uncollectedTerms;
    public final int skippedDistincts;
    public final int collectedDistincts;

    @JsonCreator
    public AggregateCountsAnswerConstraint(
        @JsonProperty("results") List<AggregateCount> results,
        @JsonProperty("aggregateTerms") Set<MiruValue> aggregateTerms,
        @JsonProperty("uncollectedTerms") Set<MiruValue> uncollectedTerms,
        @JsonProperty("skippedDistincts") int skippedDistincts,
        @JsonProperty("collectedDistincts") int collectedDistincts) {
        this.results = results;
        this.aggregateTerms = aggregateTerms;
        this.uncollectedTerms = uncollectedTerms;
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
