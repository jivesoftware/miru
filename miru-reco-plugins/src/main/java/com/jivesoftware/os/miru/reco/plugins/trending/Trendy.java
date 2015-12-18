package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import java.io.Serializable;

/**
 *
 */
public class Trendy implements Comparable<Trendy>, Serializable {

    public final MiruValue distinctValue;
    public final double rank;

    @JsonCreator
    public Trendy(
        @JsonProperty("distinctValue") MiruValue distinctValue,
        @JsonProperty("rank") double rank) {
        this.distinctValue = distinctValue;
        this.rank = rank;
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
            + '}';
    }
}
