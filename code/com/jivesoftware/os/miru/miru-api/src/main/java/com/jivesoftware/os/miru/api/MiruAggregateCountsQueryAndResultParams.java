package com.jivesoftware.os.miru.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.query.AggregateCountsQuery;
import com.jivesoftware.os.miru.api.query.result.AggregateCountsResult;

public class MiruAggregateCountsQueryAndResultParams {

    private final AggregateCountsQuery query;
    private final Optional<AggregateCountsResult> lastResult;

    @JsonCreator
    public MiruAggregateCountsQueryAndResultParams(
        @JsonProperty("query") AggregateCountsQuery query,
        @JsonProperty("lastResult") AggregateCountsResult lastResult) {
        this.query = query;
        this.lastResult = Optional.fromNullable(lastResult);
    }

    public AggregateCountsQuery getQuery() {
        return query;
    }

    public Optional<AggregateCountsResult> getLastResult() {
        return lastResult;
    }
}
