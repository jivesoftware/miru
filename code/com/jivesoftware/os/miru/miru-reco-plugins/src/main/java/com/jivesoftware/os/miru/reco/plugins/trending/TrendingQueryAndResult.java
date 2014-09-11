package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TrendingQueryAndResult {

    public final TrendingQuery query;
    public final TrendingAnswer lastResult;

    @JsonCreator
    public TrendingQueryAndResult(
            @JsonProperty("query") TrendingQuery query,
            @JsonProperty("lastResult") TrendingAnswer lastResult) {
        this.query = query;
        this.lastResult = lastResult;
    }
}
