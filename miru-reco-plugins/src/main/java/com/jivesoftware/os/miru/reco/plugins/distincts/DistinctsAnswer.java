package com.jivesoftware.os.miru.reco.plugins.distincts;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jivesoftware.os.miru.api.query.filter.MiruValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class DistinctsAnswer implements Serializable {

    public static final DistinctsAnswer EMPTY_RESULTS = new DistinctsAnswer(Collections.<MiruValue>emptyList(), 0, true);

    public final List<MiruValue> results;
    public final int collectedDistincts;
    public final boolean resultsExhausted;

    public DistinctsAnswer(
        List<MiruValue> results,
        int collectedDistincts,
        boolean resultsExhausted) {
        this.results = results;
        this.collectedDistincts = collectedDistincts;
        this.resultsExhausted = resultsExhausted;
    }

    @JsonCreator
    public static DistinctsAnswer fromJson(
        @JsonProperty("results") List<MiruValue> results,
        @JsonProperty("collectedDistincts") int collectedDistincts,
        @JsonProperty("resultsExhausted") boolean resultsExhausted) {
        return new DistinctsAnswer(new ArrayList<>(results), collectedDistincts, resultsExhausted);
    }

    @Override
    public String toString() {
        return "DistinctsAnswer{"
            + "results=" + results
            + ", collectedDistincts=" + collectedDistincts
            + ", resultsExhausted=" + resultsExhausted
            + '}';
    }

}
