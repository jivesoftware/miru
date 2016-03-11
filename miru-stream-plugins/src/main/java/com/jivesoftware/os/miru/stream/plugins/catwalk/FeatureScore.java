package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.Arrays;

/**
 *
 */
public class FeatureScore {

    public final MiruTermId[] termIds;
    public final long numerator;
    public final long denominator;

    @JsonCreator
    public FeatureScore(
        @JsonProperty("termIds") MiruTermId[] termIds,
        @JsonProperty("numerator") long numerator,
        @JsonProperty("denominator") long denominator) {
        this.termIds = termIds;
        this.numerator = numerator;
        this.denominator = denominator;
    }

    @Override
    public String toString() {
        return "FeatureScore{" +
            "termIds=" + Arrays.toString(termIds) +
            ", numerator=" + numerator +
            ", denominator=" + denominator +
            '}';
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
