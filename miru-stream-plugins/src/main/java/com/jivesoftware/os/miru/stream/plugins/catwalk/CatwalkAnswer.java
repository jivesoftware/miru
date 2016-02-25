package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import java.util.Arrays;
import java.util.List;

/** @author jonathan */
public class CatwalkAnswer {

    public static final CatwalkAnswer EMPTY_RESULTS = new CatwalkAnswer(null, true);

    public final List<FeatureScore>[] results;
    public final boolean resultsExhausted;

    @JsonCreator
    public CatwalkAnswer(
        @JsonProperty("results") List<FeatureScore>[] results,
        @JsonProperty("resultsExhausted") boolean resultsExhausted) {
        this.results = results;
        this.resultsExhausted = resultsExhausted;
    }

    @Override
    public String toString() {
        return "CatwalkAnswer{" +
            "results=" + Arrays.toString(results) +
            ", resultsExhausted=" + resultsExhausted +
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

    public static class FeatureScore {

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
}
