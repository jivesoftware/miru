package com.jivesoftware.os.miru.stream.plugins.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import java.util.Set;


/**
 * @author jonathan
 */
public class AggregateCountsReportConstraint {

    public final Set<MiruValue> aggregateTerms;
    public final Set<MiruValue> uncollectedTerms;
    public final int skippedDistincts;
    public final int collectedDistincts;

    @JsonCreator
    public AggregateCountsReportConstraint(
        @JsonProperty("aggregateTerms") Set<MiruValue> aggregateTerms,
        @JsonProperty("uncollectedTerms") Set<MiruValue> uncollectedTerms,
        @JsonProperty("skippedDistincts") int skippedDistincts,
        @JsonProperty("collectedDistincts") int collectedDistincts) {
        this.aggregateTerms = aggregateTerms;
        this.uncollectedTerms = uncollectedTerms;
        this.skippedDistincts = skippedDistincts;
        this.collectedDistincts = collectedDistincts;
    }

    @Override
    public String toString() {
        return "AggregateCountsReportConstraint{" +
            "aggregateTerms=" + aggregateTerms +
            ", uncollectedTerms=" + uncollectedTerms +
            ", skippedDistincts=" + skippedDistincts +
            ", collectedDistincts=" + collectedDistincts +
            '}';
    }
}
