package com.jivesoftware.os.miru.stream.plugins.count;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.Set;

/** @author jonathan */
public class DistinctCountResult {

    public static final DistinctCountResult EMPTY_RESULTS = new DistinctCountResult(ImmutableSet.<MiruTermId>of(), 0);

    public final ImmutableSet<MiruTermId> aggregateTerms;
    public final int collectedDistincts;

    public DistinctCountResult(ImmutableSet<MiruTermId> aggregateTerms, int collectedDistincts) {
        this.aggregateTerms = aggregateTerms;
        this.collectedDistincts = collectedDistincts;
    }

    @JsonCreator
    public static DistinctCountResult fromJson(
        @JsonProperty("aggregateTerms") Set<MiruTermId> aggregateTerms,
        @JsonProperty("collectedDistincts") int collectedDistincts) {
        return new DistinctCountResult(ImmutableSet.copyOf(aggregateTerms), collectedDistincts);
    }

    @Override
    public String toString() {
        return "DistinctCountResult{" +
            "aggregateTerms=" + aggregateTerms +
            ", collectedDistincts=" + collectedDistincts +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DistinctCountResult that = (DistinctCountResult) o;

        if (collectedDistincts != that.collectedDistincts) {
            return false;
        }
        if (aggregateTerms != null ? !aggregateTerms.equals(that.aggregateTerms) : that.aggregateTerms != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = aggregateTerms != null ? aggregateTerms.hashCode() : 0;
        result = 31 * result + collectedDistincts;
        return result;
    }
}
