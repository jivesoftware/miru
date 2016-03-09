package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import java.util.Arrays;

/**
 *
 */
public class FeatureScore {

    public final MiruValue[] values;
    public final long numerator;
    public final long denominator;

    @JsonCreator
    public FeatureScore(
        @JsonProperty("values") MiruValue[] values,
        @JsonProperty("numerator") long numerator,
        @JsonProperty("denominator") long denominator) {
        this.values = values;
        this.numerator = numerator;
        this.denominator = denominator;
    }

    @Override
    public String toString() {
        return "FeatureScore{" +
            "values=" + Arrays.toString(values) +
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
