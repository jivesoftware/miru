package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TrendingAnswerScoreSet {

    public final Map<String, List<Trendy>> results;

    @JsonCreator
    public TrendingAnswerScoreSet(@JsonProperty("results") Map<String, List<Trendy>> results) {
        this.results = results;
    }

    @Override
    public String toString() {
        return "TrendingAnswerScoreSet{" +
            "results=" + results +
            '}';
    }
}
