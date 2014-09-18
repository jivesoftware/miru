package com.jivesoftware.os.miru.plugin.solution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author jonathan.colt
 */
public class MiruRequestAndReport<Q, R> {

    public final MiruRequest<Q> request;
    public final R report;

    @JsonCreator
    public MiruRequestAndReport(
            @JsonProperty("request") MiruRequest<Q> request,
            @JsonProperty("report") R report) {
        this.request = request;
        this.report = report;
    }
}
