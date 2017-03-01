package com.jivesoftware.os.miru.catwalk.shared;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.io.Serializable;
import java.util.Arrays;

/**
 *
 */
public class FeatureScore implements Serializable {

    public final MiruTermId[] termIds;
    public final long[] numerators;
    public final long denominator;
    public final int numPartitions;

    @JsonCreator
    public FeatureScore(
        @JsonProperty("termIds") MiruTermId[] termIds,
        @JsonProperty("numerators") long[] numerators,
        @JsonProperty("denominator") long denominator,
        @JsonProperty("numPartitions")  int numPartitions) {
        this.termIds = termIds;
        this.numerators = numerators;
        this.denominator = denominator;
        this.numPartitions = numPartitions;
    }

    @Override
    public String toString() {
        return "FeatureScore{"
            + "termIds=" + Arrays.toString(termIds)
            + ", numerators=" + Arrays.toString(numerators)
            + ", denominator=" + denominator
            + ", numPartitions=" + numPartitions
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException("NOPE");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("NOPE");
    }
}
