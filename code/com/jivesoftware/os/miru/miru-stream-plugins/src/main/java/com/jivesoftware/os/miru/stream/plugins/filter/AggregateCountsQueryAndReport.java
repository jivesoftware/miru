package com.jivesoftware.os.miru.stream.plugins.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AggregateCountsQueryAndReport {

    public final AggregateCountsQuery query;
    public final AggregateCountsReport report;

    @JsonCreator
    public AggregateCountsQueryAndReport(
            @JsonProperty("query") AggregateCountsQuery query,
            @JsonProperty("report") AggregateCountsReport report) {
        this.query = query;
        this.report = report;
    }
}
