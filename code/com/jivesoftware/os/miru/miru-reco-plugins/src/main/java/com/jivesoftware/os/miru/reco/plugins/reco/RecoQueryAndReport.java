package com.jivesoftware.os.miru.reco.plugins.reco;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RecoQueryAndReport {

    public final RecoQuery query;
    public final RecoReport report;

    @JsonCreator
    public RecoQueryAndReport(
            @JsonProperty("query") RecoQuery query,
            @JsonProperty("report") RecoReport report) {
        this.query = query;
        this.report = report;
    }
}
