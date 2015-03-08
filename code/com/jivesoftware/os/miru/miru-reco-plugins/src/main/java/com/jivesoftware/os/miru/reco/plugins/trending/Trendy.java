package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Arrays;

/**
 *
 */
public class Trendy implements Comparable<Trendy>, Serializable {

    public final String distinctValue;
    public final double rank;
    public final Double rankDelta; // nullable
    public final long[] waveform;

    @JsonCreator
    public Trendy(
        @JsonProperty("distinctValue") String distinctValue,
        @JsonProperty("rank") double rank,
        @JsonProperty("rankDelta") Double rankDelta,
        @JsonProperty("waveform") long[] waveform) {
        this.distinctValue = distinctValue;
        this.rank = rank;
        this.rankDelta = rankDelta;
        this.waveform = waveform;
    }

    @Override
    public int compareTo(Trendy o) {
        // reversed for descending order
        return Double.compare(o.rank, rank);
    }

    @Override
    public String toString() {
        return "Trendy{"
            + "distinctValue=" + distinctValue
            + ", rank=" + rank
            + ", waveform=" + Arrays.toString(waveform)
            + '}';
    }
}
