package com.jivesoftware.os.miru.stream.plugins.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.Set;


/**
 *
 * @author jonathan
 */
public class AggregateCountsReport {

    public final ImmutableSet<MiruTermId> aggregateTerms;
    public final int skippedDistincts;
    public final int collectedDistincts;

    public AggregateCountsReport(ImmutableSet<MiruTermId> aggregateTerms, int skippedDistincts, int collectedDistincts) {
        this.aggregateTerms = aggregateTerms;
        this.skippedDistincts = skippedDistincts;
        this.collectedDistincts = collectedDistincts;
    }

    @JsonCreator
    public static AggregateCountsReport fromJson(
            @JsonProperty("aggregateTerms") Set<MiruTermId> aggregateTerms,
            @JsonProperty("skippedDistincts") int skippedDistincts,
            @JsonProperty("collectedDistincts") int collectedDistincts) {
        return new AggregateCountsReport(ImmutableSet.copyOf(aggregateTerms), skippedDistincts, collectedDistincts);
    }

    @Override
    public String toString() {
        return "AggregateCountsReport{" +
                "aggregateTerms=" + aggregateTerms +
                ", skippedDistincts=" + skippedDistincts +
                ", collectedDistincts=" + collectedDistincts +
                '}';
    }
}
