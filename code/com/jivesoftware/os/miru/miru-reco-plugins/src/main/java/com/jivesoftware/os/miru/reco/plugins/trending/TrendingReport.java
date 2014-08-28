package com.jivesoftware.os.miru.reco.plugins.trending;

import com.google.common.collect.ImmutableSet;
import com.jivesoftware.os.miru.api.base.MiruTermId;

/**
 *
 */
public class TrendingReport {

    public final int collectedDistincts;
    public final ImmutableSet<MiruTermId> aggregateTerms;

    public TrendingReport(int collectedDistincts, ImmutableSet<MiruTermId> aggregateTerms) {
        this.collectedDistincts = collectedDistincts;
        this.aggregateTerms = aggregateTerms;
    }

    @Override
    public String toString() {
        return "TrendingReport{" +
            ", collectedDistincts=" + collectedDistincts +
            ", aggregateTerms=" + aggregateTerms +
            '}';
    }
}
