package com.jivesoftware.os.miru.reco.plugins.trending;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TrendingQueryAndReport {

    public final TrendingQuery query;
    public final TrendingReport report;

    @JsonCreator
    public TrendingQueryAndReport(
            @JsonProperty("query") TrendingQuery query,
            @JsonProperty("report") TrendingReport report) {
        this.query = query;
        this.report = report;
    }
}
