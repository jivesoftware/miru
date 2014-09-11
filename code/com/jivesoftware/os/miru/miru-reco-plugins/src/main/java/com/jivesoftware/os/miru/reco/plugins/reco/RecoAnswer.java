package com.jivesoftware.os.miru.reco.plugins.reco;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import java.util.List;

/**
 *
 */
public class RecoAnswer {

    public static final RecoAnswer EMPTY_RESULTS = new RecoAnswer(ImmutableList.<Recommendation>of());

    public final ImmutableList<Recommendation> results;

    public RecoAnswer(List<Recommendation> results) {
        this.results = ImmutableList.copyOf(results);
    }

    @JsonCreator
    public static RecoAnswer fromJson(
            @JsonProperty("results") List<Recommendation> results) {
        return new RecoAnswer(ImmutableList.copyOf(results));
    }

    @Override
    public String toString() {
        return "RecoAnswer{"
                + "results=" + results
                + '}';
    }

    public static class Recommendation implements Comparable<Recommendation> {

        public final MiruTermId distinctValue;
        public final float rank;

        public Recommendation(MiruTermId distinctValue, float rank) {
            this.distinctValue = distinctValue;
            this.rank = rank;
        }

        @JsonCreator
        public static Recommendation fromJson(
                @JsonProperty("distinctValue") MiruTermId distinctValue,
                @JsonProperty("rank") float rank)
                throws Exception {
            return new Recommendation(distinctValue, rank);
        }

        @Override
        public int compareTo(Recommendation o) {
            // reversed for descending order
            return Double.compare(o.rank, rank);
        }

        @Override
        public String toString() {
            return "Recommendation{"
                    + ", distinctValue=" + distinctValue
                    + ", rank=" + rank
                    + '}';
        }
    }

}
