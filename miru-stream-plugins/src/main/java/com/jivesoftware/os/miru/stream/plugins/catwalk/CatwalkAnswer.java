package com.jivesoftware.os.miru.stream.plugins.catwalk;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/** @author jonathan */
public class CatwalkAnswer implements Serializable {

    public static final CatwalkAnswer EMPTY_RESULTS = new CatwalkAnswer(null, null, -1L, MiruTimeRange.ALL_TIME, true, false, false);
    public static final CatwalkAnswer DESTROYED_RESULTS = new CatwalkAnswer(null, null, -1L, MiruTimeRange.ALL_TIME, true, false, true);

    public final List<FeatureScore>[] results;
    public final long[] modelCounts;
    public final long totalCount;
    public final MiruTimeRange timeRange;
    public final boolean resultsExhausted;
    public final boolean resultsClosed;
    public final boolean destroyed;

    @JsonCreator
    public CatwalkAnswer(
        @JsonProperty("results") List<FeatureScore>[] results,
        @JsonProperty("modelCounts") long[] modelCounts,
        @JsonProperty("totalCount") long totalCount,
        @JsonProperty("timeRange") MiruTimeRange timeRange,
        @JsonProperty("resultsExhausted") boolean resultsExhausted,
        @JsonProperty("resultsClosed") boolean resultsClosed,
        @JsonProperty("destroyed") boolean destroyed) {
        this.results = results;
        this.modelCounts = modelCounts;
        this.totalCount = totalCount;
        this.timeRange = timeRange;
        this.resultsExhausted = resultsExhausted;
        this.resultsClosed = resultsClosed;
        this.destroyed = destroyed;
    }

    @Override
    public String toString() {
        return "CatwalkAnswer{" +
            "results=" + Arrays.toString(results) +
            ", modelCounts=" + Arrays.toString(modelCounts) +
            ", totalCount=" + totalCount +
            ", timeRange=" + timeRange +
            ", resultsExhausted=" + resultsExhausted +
            ", resultsClosed=" + resultsClosed +
            ", destroyed=" + destroyed +
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
