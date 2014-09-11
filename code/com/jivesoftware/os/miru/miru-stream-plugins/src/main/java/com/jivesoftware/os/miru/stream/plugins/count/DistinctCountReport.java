package com.jivesoftware.os.miru.stream.plugins.count;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.Set;

/**
 *
 * @author jonathan
 */
public class DistinctCountReport {

    public final ImmutableSet<MiruTermId> aggregateTerms;
    public final int collectedDistincts;

    public DistinctCountReport(ImmutableSet<MiruTermId> aggregateTerms, int collectedDistincts) {
        this.aggregateTerms = aggregateTerms;
        this.collectedDistincts = collectedDistincts;
    }

    @JsonCreator
    public static DistinctCountReport fromJson(
            @JsonProperty("aggregateTerms") Set<MiruTermId> aggregateTerms,
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
