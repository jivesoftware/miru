package com.jivesoftware.os.miru.stream.plugins.count;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DistinctCountQueryAndResult {

    public final DistinctCountQuery query;
    public final DistinctCountAnswer lastResult;

    @JsonCreator
    public DistinctCountQueryAndResult(
            @JsonProperty("query") DistinctCountQuery query,
            @JsonProperty("lastResult") DistinctCountAnswer lastResult) {
        this.query = query;
        this.lastResult = lastResult;
    }
}
