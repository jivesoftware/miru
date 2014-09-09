package com.jivesoftware.os.miru.stream.plugins.count;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DistinctCountQueryAndResult {

    public final DistinctCountQuery query;
    public final DistinctCountResult lastResult;

    @JsonCreator
    public DistinctCountQueryAndResult(
            @JsonProperty("query") DistinctCountQuery query,
            @JsonProperty("lastResult") DistinctCountResult lastResult) {
        this.query = query;
        this.lastResult = lastResult;
    }
}
