package com.jivesoftware.os.miru.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.query.DistinctCountQuery;
import com.jivesoftware.os.miru.api.query.result.DistinctCountResult;

public class MiruDistinctCountQueryAndResultParams {

    private final DistinctCountQuery query;
    private final Optional<DistinctCountResult> lastResult;

    @JsonCreator
    public MiruDistinctCountQueryAndResultParams(
        @JsonProperty("query") DistinctCountQuery query,
        @JsonProperty("lastResult") DistinctCountResult lastResult) {
        this.query = query;
        this.lastResult = Optional.fromNullable(lastResult);
    }

    public DistinctCountQuery getQuery() {
        return query;
    }

    public Optional<DistinctCountResult> getLastResult() {
        return lastResult;
    }
}
