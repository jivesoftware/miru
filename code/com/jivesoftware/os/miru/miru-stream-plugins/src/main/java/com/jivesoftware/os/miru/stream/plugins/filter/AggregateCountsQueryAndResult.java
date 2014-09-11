package com.jivesoftware.os.miru.stream.plugins.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AggregateCountsQueryAndResult {

    public final AggregateCountsQuery query;
    public final AggregateCountsAnswer lastResult;

    @JsonCreator
    public AggregateCountsQueryAndResult(
            @JsonProperty("query") AggregateCountsQuery query,
            @JsonProperty("lastResult") AggregateCountsAnswer lastResult) {
        this.query = query;
        this.lastResult = lastResult;
    }
}
