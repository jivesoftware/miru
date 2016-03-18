package com.jivesoftware.os.miru.stream.plugins.strut;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.List;

/** @author jonathan */
public class StrutAnswer implements Serializable {

    public static final StrutAnswer EMPTY_RESULTS = new StrutAnswer(null, 0f, true);

    public final List<HotOrNot> results;
    public final float threshold;
    public final boolean resultsExhausted;

    @JsonCreator
    public StrutAnswer(
        @JsonProperty("results") List<HotOrNot> results,
        @JsonProperty("threshold") float threshold,
        @JsonProperty("resultsExhausted") boolean resultsExhausted) {
        this.results = results;
        this.threshold = threshold;
        this.resultsExhausted = resultsExhausted;
    }

    @Override
    public String toString() {
        return "StrutAnswer{" +
            "results=" + results +
            ", threshold=" + threshold +
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

}
