package com.jivesoftware.os.miru.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.query.RecoQuery;
import com.jivesoftware.os.miru.api.query.result.RecoResult;

public class MiruRecoQueryAndResultParams {

    private final RecoQuery query;
    private final Optional<RecoResult> lastResult;

    @JsonCreator
    public MiruRecoQueryAndResultParams(
        @JsonProperty("query") RecoQuery query,
        @JsonProperty("lastResult") RecoResult lastResult) {
        this.query = query;
        this.lastResult = Optional.fromNullable(lastResult);
    }

    public RecoQuery getQuery() {
        return query;
    }

    public Optional<RecoResult> getLastResult() {
        return lastResult;
    }
}
