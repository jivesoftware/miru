package com.jivesoftware.os.miru.stream.plugins.count;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import java.util.Set;

/**
 *
 * @author jonathan
 */
public class DistinctCountReport {

    public final ImmutableSet<String> aggregateTerms;
    public final int collectedDistincts;

    public DistinctCountReport(ImmutableSet<String> aggregateTerms, int collectedDistincts) {
        this.aggregateTerms = aggregateTerms;
        this.collectedDistincts = collectedDistincts;
    }

    @JsonCreator
    public static DistinctCountReport fromJson(
            @JsonProperty("aggregateTerms") Set<String> aggregateTerms,
            @JsonProperty("collectedDistincts") int collectedDistincts) {
        return new DistinctCountReport(ImmutableSet.copyOf(aggregateTerms), collectedDistincts);
    }

    @Override
    public String toString() {
        return "DistinctCountReport{" +
                "aggregateTerms=" + aggregateTerms +
                ", collectedDistincts=" + collectedDistincts +
                '}';
    }
}
