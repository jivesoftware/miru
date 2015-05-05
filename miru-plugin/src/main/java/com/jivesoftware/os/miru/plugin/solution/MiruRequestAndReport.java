package com.jivesoftware.os.miru.plugin.solution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

/**
 * @author jonathan.colt
 * @param <Q>
 * @param <R>
 */
public class MiruRequestAndReport<Q, R> implements Serializable {

    public final MiruRequest<Q> request;
    public final R report;

    @JsonCreator
    public MiruRequestAndReport(
        @JsonProperty("request") MiruRequest<Q> request,
        @JsonProperty("report") R report) {
        this.request = request;
        this.report = report;
    }

    @Override
    public String toString() {
        return "MiruRequestAndReport{" + "request=" + request + ", report=" + report + '}';
    }

}
