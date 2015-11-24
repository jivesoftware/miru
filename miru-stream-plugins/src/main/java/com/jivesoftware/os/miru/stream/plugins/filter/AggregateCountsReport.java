package com.jivesoftware.os.miru.stream.plugins.filter;

import java.util.Map;

/**
 *
 * @author jonathan
 */
public class AggregateCountsReport {

    public final Map<String, AggregateCountsReportConstraint> constraints;

    public AggregateCountsReport(Map<String, AggregateCountsReportConstraint> constraints) {
        this.constraints = constraints;
    }

    @Override
    public String toString() {
        return "AggregateCountsReport{"
            + "constraints=" + constraints
            + '}';
    }
}
