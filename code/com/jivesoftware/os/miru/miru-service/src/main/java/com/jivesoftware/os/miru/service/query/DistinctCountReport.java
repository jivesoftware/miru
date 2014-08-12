package com.jivesoftware.os.miru.service.query;

import com.google.common.collect.ImmutableSet;
import com.jivesoftware.os.miru.api.base.MiruTermId;

/**
 *
 * @author jonathan
 */
public class DistinctCountReport {

    public final int collectedDistincts;
    public final ImmutableSet<MiruTermId> aggregateTerms;

    public DistinctCountReport(int collectedDistincts, ImmutableSet<MiruTermId> aggregateTerms) {
        this.collectedDistincts = collectedDistincts;
        this.aggregateTerms = aggregateTerms;
    }

    @Override
    public String toString() {
        return "AggregateCountsReport{" +
                ", collectedDistincts=" + collectedDistincts +
                ", aggregateTerms=" + aggregateTerms +
                '}';
    }
}
