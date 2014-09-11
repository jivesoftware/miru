package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.Set;

/**
 *
 */
public class TrendingReport {

    public final ImmutableSet<MiruTermId> aggregateTerms;
    public final int collectedDistincts;

    public TrendingReport(ImmutableSet<MiruTermId> aggregateTerms, int collectedDistincts) {
        this.aggregateTerms = aggregateTerms;
        this.collectedDistincts = collectedDistincts;
    }

    @JsonCreator
    public static TrendingReport fromJson(
            @JsonProperty("aggregateTerms") Set<MiruTermId> aggregateTerms,
            @JsonProperty("collectedDistincts") int collectedDistincts) {
        return new TrendingReport(ImmutableSet.copyOf(aggregateTerms), collectedDistincts);
    }

    @Override
    public String toString() {
        return "TrendingReport{" +
            ", collectedDistincts=" + collectedDistincts +
            ", aggregateTerms=" + aggregateTerms +
            '}';
    }
}
