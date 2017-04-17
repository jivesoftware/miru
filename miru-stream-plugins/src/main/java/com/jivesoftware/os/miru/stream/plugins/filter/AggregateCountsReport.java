package com.jivesoftware.os.miru.stream.plugins.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * @author jonathan
 */
public class AggregateCountsReport {

    public final Map<String, AggregateCountsReportConstraint> constraints;

    @JsonCreator
    public AggregateCountsReport(@JsonProperty("constraints") Map<String, AggregateCountsReportConstraint> constraints) {
        this.constraints = constraints;
    }

    @Override
    public String toString() {
        return "AggregateCountsReport{"
            + "constraints=" + constraints
            + '}';
    }
}
