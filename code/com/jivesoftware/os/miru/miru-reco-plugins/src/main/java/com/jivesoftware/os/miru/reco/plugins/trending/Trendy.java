package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.filer.io.FilerIO;
import java.io.Serializable;
import java.util.Arrays;

/**
 *
 */
public class Trendy implements Comparable<Trendy>, Serializable {

    public final byte[] distinctValue;
    public final double rank;
    public final long[] waveform;

    @JsonCreator
    public Trendy(
        @JsonProperty("distinctValue") byte[] distinctValue,
        @JsonProperty("rank") double rank,
        @JsonProperty("waveform") long[] waveform) {
        this.distinctValue = distinctValue;
        this.rank = rank;
        this.waveform = waveform;
    }

    @Override
    public int compareTo(Trendy o) {
        // reversed for descending order
        return Double.compare(o.rank, rank);
    }

    @Override
    public String toString() {
        String v = (distinctValue.length == 4)
            ? String.valueOf(FilerIO.bytesInt(distinctValue)) : (distinctValue.length == 8)
            ? String.valueOf(FilerIO.bytesLong(distinctValue)) : Arrays.toString(distinctValue);
        return "Trendy{"
            + "distinctValue=" + v
            + ", rank=" + rank
            + ", waveform=" + Arrays.toString(waveform)
            + '}';
    }
}
