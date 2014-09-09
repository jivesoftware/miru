package com.jivesoftware.os.miru.reco.plugins.reco;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RecoQueryAndResult {

    public final RecoQuery query;
    public final RecoResult lastResult;

    @JsonCreator
    public RecoQueryAndResult(
            @JsonProperty("query") RecoQuery query,
            @JsonProperty("lastResult") RecoResult lastResult) {
        this.query = query;
        this.lastResult = lastResult;
    }
}
