package com.jivesoftware.os.miru.stream.plugins.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import java.util.List;
import java.util.Set;

/** @author jonathan */
public class AggregateCountsAnswer {

    public static final AggregateCountsAnswer EMPTY_RESULTS = new AggregateCountsAnswer(ImmutableList.<AggregateCount>of(),
        ImmutableSet.<String>of(), 0, 0, true);

    public final ImmutableList<AggregateCount> results;
    public final ImmutableSet<String> aggregateTerms;
    public final int skippedDistincts;
    public final int collectedDistincts;
    public final boolean resultsExhausted;

    public AggregateCountsAnswer(
        ImmutableList<AggregateCount> results,
        ImmutableSet<String> aggregateTerms,
        int skippedDistincts,
        int collectedDistincts,
        boolean resultsExhausted) {
        this.results = results;
        this.aggregateTerms = aggregateTerms;
        this.skippedDistincts = skippedDistincts;
        this.collectedDistincts = collectedDistincts;
        this.resultsExhausted = resultsExhausted;
    }

    @JsonCreator
    public static AggregateCountsAnswer fromJson(
        @JsonProperty("results") List<AggregateCount> results,
        @JsonProperty("aggregateTerms") Set<String> aggregateTerms,
        @JsonProperty("skippedDistincts") int skippedDistincts,
        @JsonProperty("collectedDistincts") int collectedDistincts,
        @JsonProperty("resultsExhausted") boolean resultsExhausted) {
        return new AggregateCountsAnswer(ImmutableList.copyOf(results),
            ImmutableSet.copyOf(aggregateTerms),
            skippedDistincts,
            collectedDistincts,
            resultsExhausted);
    }

    @Override
    public String toString() {
        return "AggregateCountsAnswer{" +
            "results=" + results +
            ", aggregateTerms=" + aggregateTerms +
            ", skippedDistincts=" + skippedDistincts +
            ", collectedDistincts=" + collectedDistincts +
            ", resultsExhausted=" + resultsExhausted +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AggregateCountsAnswer that = (AggregateCountsAnswer) o;

        if (skippedDistincts != that.skippedDistincts) {
            return false;
        }
        if (collectedDistincts != that.collectedDistincts) {
            return false;
        }
        if (resultsExhausted != that.resultsExhausted) {
            return false;
        }
        if (results != null ? !results.equals(that.results) : that.results != null) {
            return false;
        }
        return !(aggregateTerms != null ? !aggregateTerms.equals(that.aggregateTerms) : that.aggregateTerms != null);

    }

    @Override
    public int hashCode() {
        int result = results != null ? results.hashCode() : 0;
        result = 31 * result + (aggregateTerms != null ? aggregateTerms.hashCode() : 0);
        result = 31 * result + skippedDistincts;
        result = 31 * result + collectedDistincts;
        result = 31 * result + (resultsExhausted ? 1 : 0);
        return result;
    }

    public static class AggregateCount {

        public final MiruActivity mostRecentActivity;
        public final String distinctValue;
        public final long count;
        public boolean unread;

        @JsonCreator
        public AggregateCount(
            @JsonProperty("mostRecentActivity") MiruActivity mostRecentActivity,
            @JsonProperty("distinctValue") String distinctValue,
            @JsonProperty("count") long count,
            @JsonProperty("unread") boolean unread) {
            this.mostRecentActivity = mostRecentActivity;
            this.distinctValue = distinctValue;
            this.count = count;
            this.unread = unread;
        }

        public void setUnread(boolean unread) {
            this.unread = unread;
        }

        @Override
        public String toString() {
            return "AggregateCount{" +
                "mostRecentActivity=" + mostRecentActivity +
                ", distinctValue=" + distinctValue +
                ", count=" + count +
                ", unread=" + unread +
                '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            AggregateCount that = (AggregateCount) o;

            if (count != that.count) {
                return false;
            }
            if (unread != that.unread) {
                return false;
            }
            if (distinctValue != null ? !distinctValue.equals(that.distinctValue) : that.distinctValue != null) {
                return false;
            }
            if (mostRecentActivity != null ? !mostRecentActivity.equals(that.mostRecentActivity) : that.mostRecentActivity != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = mostRecentActivity != null ? mostRecentActivity.hashCode() : 0;
            result = 31 * result + (distinctValue != null ? distinctValue.hashCode() : 0);
            result = 31 * result + (int) (count ^ (count >>> 32));
            result = 31 * result + (unread ? 1 : 0);
            return result;
        }
    }
}
