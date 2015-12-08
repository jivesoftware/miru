package com.jivesoftware.os.miru.stream.plugins.count;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import java.util.Set;

/**
 * @author jonathan
 */
public class DistinctCountReport {

    public final Set<MiruValue> aggregateTerms;
    public final int collectedDistincts;

    @JsonCreator
    public DistinctCountReport(
        @JsonProperty("aggregateTerms") Set<MiruValue> aggregateTerms,
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
