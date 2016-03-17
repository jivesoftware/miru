package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/** @author jonathan */
public class CatwalkAnswer implements Serializable {

    public static final CatwalkAnswer EMPTY_RESULTS = new CatwalkAnswer(null, -1L, -1L, MiruTimeRange.ALL_TIME, true, false);

    public final List<FeatureScore>[] results;
    public final long modelCount;
    public final long totalCount;
    public final MiruTimeRange timeRange;
    public final boolean resultsExhausted;
    public final boolean resultsClosed;

    @JsonCreator
    public CatwalkAnswer(
        @JsonProperty("results") List<FeatureScore>[] results,
        @JsonProperty("modelCount") long modelCount,
        @JsonProperty("totalCount") long totalCount,
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("resultsExhausted") boolean resultsExhausted,
        @JsonProperty("resultsClosed") boolean resultsClosed) {
        this.results = results;
        this.modelCount = modelCount;
        this.totalCount = totalCount;
        this.timeRange = timeRange;
        this.resultsExhausted = resultsExhausted;
        this.resultsClosed = resultsClosed;
    }

    @Override
    public String toString() {
        return "CatwalkAnswer{" +
            "results=" + Arrays.toString(results) +
            ", modelCount=" + modelCount +
            ", totalCount=" + totalCount +
            ", timeRange=" + timeRange +
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
