package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

public class MiruTrendingQueryAndResultParams {

    private final TrendingQuery query;
    private final Optional<TrendingResult> lastResult;

    @JsonCreator
    public MiruTrendingQueryAndResultParams(
        @JsonProperty("query") TrendingQuery query,
        @JsonProperty("lastResult") TrendingResult lastResult) {
        this.query = query;
        this.lastResult = Optional.fromNullable(lastResult);
    }

    public TrendingQuery getQuery() {
        return query;
    }

    public Optional<TrendingResult> getLastResult() {
        return lastResult;
    }
}
