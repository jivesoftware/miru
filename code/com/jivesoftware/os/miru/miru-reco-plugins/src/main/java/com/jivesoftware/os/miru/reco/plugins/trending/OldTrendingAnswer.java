package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.reco.trending.SimpleRegressionTrend;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class OldTrendingAnswer implements Serializable {

    public static final OldTrendingAnswer EMPTY_RESULTS = new OldTrendingAnswer(Collections.<Trendy>emptyList(),
        Collections.<MiruTermId>emptySet(), 0, true);

    public final List<Trendy> results;
    public final Set<MiruTermId> aggregateTerms;
    public final int collectedDistincts;
    public final boolean resultsExhausted;

    public OldTrendingAnswer(
        List<Trendy> results,
        Set<MiruTermId> aggregateTerms,
        int collectedDistincts,
        boolean resultsExhausted) {
        this.results = results;
        this.aggregateTerms = aggregateTerms;
        this.collectedDistincts = collectedDistincts;
        this.resultsExhausted = resultsExhausted;
    }

    @JsonCreator
    public static OldTrendingAnswer fromJson(
        @JsonProperty ("results") List<Trendy> results,
        @JsonProperty ("aggregateTerms") Set<MiruTermId> aggregateTerms,
        @JsonProperty ("collectedDistincts") int collectedDistincts,
        @JsonProperty ("resultsExhausted") boolean resultsExhausted) {
        return new OldTrendingAnswer(new ArrayList<>(results), new HashSet<>(aggregateTerms), collectedDistincts, resultsExhausted);
    }

    @Override
    public String toString() {
        return "OldTrendingAnswer{"
            + "results=" + results
            + ", aggregateTerms=" + aggregateTerms
            + ", collectedDistincts=" + collectedDistincts
            + ", resultsExhausted=" + resultsExhausted
            + '}';
    }

    public static class Trendy implements Comparable<Trendy>, Serializable {

        public final byte[] distinctValue;
        public final SimpleRegressionTrend trend;
        public final double rank;

        public Trendy(byte[] distinctValue, SimpleRegressionTrend trend, double rank) {
            this.distinctValue = distinctValue;
            this.trend = trend;
            this.rank = rank;
        }

        @JsonCreator
        public static Trendy fromJson(
            @JsonProperty ("distinctValue") byte[] distinctValue,
            @JsonProperty ("trend") byte[] trendBytes,
            @JsonProperty ("rank") double rank)
            throws Exception {
            return new Trendy(distinctValue, new SimpleRegressionTrend(trendBytes), rank);
        }

        @JsonGetter ("trend")
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
            return "Trendy{"
                + "distinctValue=" + v
                + ", trend=" + trend
                + ", rank=" + rank
                + '}';
        }
    }

}
