package com.jivesoftware.os.miru.stream.plugins.count;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DistinctCountQueryAndReport {

    public final DistinctCountQuery query;
    public final DistinctCountReport report;

    @JsonCreator
    public DistinctCountQueryAndReport(
            @JsonProperty("query") DistinctCountQuery query,
            @JsonProperty("report") DistinctCountReport report) {
        this.query = query;
        this.report = report;
    }
}
