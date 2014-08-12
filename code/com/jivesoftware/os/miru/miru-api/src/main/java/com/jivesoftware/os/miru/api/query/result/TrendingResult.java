package com.jivesoftware.os.miru.api.query.result;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.reco.trending.SimpleRegressionTrend;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class TrendingResult {

    public static final TrendingResult EMPTY_RESULTS = new TrendingResult(ImmutableList.<Trendy>of(),
        ImmutableSet.<MiruTermId>of(), 0);

    public final ImmutableList<Trendy> results;
    public final ImmutableSet<MiruTermId> aggregateTerms;
    public final int collectedDistincts;

    public TrendingResult(
        ImmutableList<Trendy> results,
        ImmutableSet<MiruTermId> aggregateTerms,
        int collectedDistincts) {
        this.results = results;
        this.aggregateTerms = aggregateTerms;
        this.collectedDistincts = collectedDistincts;
    }

    @JsonCreator
    public static TrendingResult fromJson(
        @JsonProperty("results") List<Trendy> results,
        @JsonProperty("aggregateTerms") Set<MiruTermId> aggregateTerms,
        @JsonProperty("collectedDistincts") int collectedDistincts) {
        return new TrendingResult(ImmutableList.copyOf(results), ImmutableSet.copyOf(aggregateTerms), collectedDistincts);
    }

    @Override
    public String toString() {
        return "TrendingResult{" +
            "results=" + results +
            ", aggregateTerms=" + aggregateTerms +
            ", collectedDistincts=" + collectedDistincts +
            '}';
    }

    public static class Trendy implements Comparable<Trendy> {

        public final MiruActivity mostRecentActivity;
        public final byte[] distinctValue;
        public final SimpleRegressionTrend trend;
        public final double rank;

        public Trendy(MiruActivity mostRecentActivity, byte[] distinctValue, SimpleRegressionTrend trend, double rank) {
            this.mostRecentActivity = mostRecentActivity;
            this.distinctValue = distinctValue;
            this.trend = trend;
            this.rank = rank;
        }

        @JsonCreator
        public static Trendy fromJson(
            @JsonProperty("mostRecentActivity") MiruActivity mostRecentActivity,
            @JsonProperty("distinctValue") byte[] distinctValue,
            @JsonProperty("trend") byte[] trendBytes,
            @JsonProperty("rank") double rank)
            throws Exception {

            SimpleRegressionTrend trend = new SimpleRegressionTrend();
            trend.initWithBytes(trendBytes);

            return new Trendy(mostRecentActivity, distinctValue, trend, rank);
        }

        @JsonGetter("trend")
        public byte[] getTrendAsBytes() throws Exception {
            return trend.toBytes();
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
            return "Trendy{" +
                "mostRecentActivity=" + mostRecentActivity +
                ", distinctValue=" + v +
                ", trend=" + trend +
                ", rank=" + rank +
                '}';
        }
    }

}
