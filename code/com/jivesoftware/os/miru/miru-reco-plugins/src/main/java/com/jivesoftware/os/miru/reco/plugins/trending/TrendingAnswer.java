package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class TrendingAnswer implements Serializable {

    public static final TrendingAnswer EMPTY_RESULTS = new TrendingAnswer(Collections.<Trendy>emptyList());

    public final List<Trendy> results;

    @JsonCreator
    public TrendingAnswer(@JsonProperty("results") List<Trendy> results) {
        this.results = results;
    }

    @Override
    public String toString() {
        return "TrendingAnswer{"
            + "results=" + results
            + '}';
    }
}
