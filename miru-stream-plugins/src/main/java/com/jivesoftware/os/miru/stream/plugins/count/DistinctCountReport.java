package com.jivesoftware.os.miru.stream.plugins.count;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Set;

/**
 * @author jonathan
 */
public class DistinctCountReport {

    public final Set<String> aggregateTerms;
    public final int collectedDistincts;

    @JsonCreator
    public DistinctCountReport(
        @JsonProperty("aggregateTerms") Set<String> aggregateTerms,
        @JsonProperty("collectedDistincts") int collectedDistincts) {
        this.aggregateTerms = aggregateTerms;
        this.collectedDistincts = collectedDistincts;
    }

    @Override
    public String toString() {
        return "DistinctCountReport{" +
            "aggregateTerms=" + aggregateTerms +
            ", collectedDistincts=" + collectedDistincts +
            '}';
    }
}
