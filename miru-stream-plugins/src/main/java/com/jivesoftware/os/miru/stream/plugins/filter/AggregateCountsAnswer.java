package com.jivesoftware.os.miru.stream.plugins.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * @author jonathan
 */
public class AggregateCountsAnswer {

    public static final AggregateCountsAnswer EMPTY_RESULTS = new AggregateCountsAnswer(ImmutableMap.of(), true);

    public final Map<String, AggregateCountsAnswerConstraint> constraints;
    public final boolean resultsExhausted;

    @JsonCreator
    public AggregateCountsAnswer(
        @JsonProperty("constraints") Map<String, AggregateCountsAnswerConstraint> constraints,
        @JsonProperty("resultsExhausted") boolean resultsExhausted) {
        this.constraints = constraints;
        this.resultsExhausted = resultsExhausted;
    }

    @Override
    public String toString() {
        return "AggregateCountsAnswer{"
            + "constraints=" + constraints
            + ", resultsExhausted=" + resultsExhausted
            + '}';
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
