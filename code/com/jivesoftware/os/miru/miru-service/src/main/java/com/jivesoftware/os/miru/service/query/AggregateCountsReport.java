package com.jivesoftware.os.miru.service.query;

import com.google.common.collect.ImmutableSet;
import com.jivesoftware.os.miru.api.base.MiruTermId;


/**
 *
 * @author jonathan
 */
public class AggregateCountsReport {

    public final int skippedDistincts;
    public final int collectedDistincts;
    public final ImmutableSet<MiruTermId> aggregateTerms;

    public AggregateCountsReport(int skippedDistincts, int collectedDistincts, ImmutableSet<MiruTermId> aggregateTerms) {
        this.skippedDistincts = skippedDistincts;
        this.collectedDistincts = collectedDistincts;
        this.aggregateTerms = aggregateTerms;
    }

    @Override
    public String toString() {
        return "AggregateCountsReport{" +
                "skippedDistincts=" + skippedDistincts +
                ", collectedDistincts=" + collectedDistincts +
                ", aggregateTerms=" + aggregateTerms +
                '}';
    }
}
