package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.util.Arrays;
import java.util.List;

/** @author jonathan */
public class CatwalkAnswer {

    public static final CatwalkAnswer EMPTY_RESULTS = new CatwalkAnswer(null, MiruTimeRange.ALL_TIME, true, false);

    public final List<FeatureScore>[] results;
    public final MiruTimeRange timeRange;
    public final boolean resultsExhausted;
    public final boolean resultsClosed;

    @JsonCreator
    public CatwalkAnswer(
        @JsonProperty("results") List<FeatureScore>[] results,
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("resultsExhausted") boolean resultsExhausted,
        @JsonProperty("resultsClosed") boolean resultsClosed) {
        this.results = results;
        this.timeRange = timeRange;
        this.resultsExhausted = resultsExhausted;
        this.resultsClosed = resultsClosed;
    }

    @Override
    public String toString() {
        return "CatwalkAnswer{" +
            "results=" + Arrays.toString(results) +
            ", resultsExhausted=" + resultsExhausted +
            ", resultsClosed=" + resultsClosed +
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
