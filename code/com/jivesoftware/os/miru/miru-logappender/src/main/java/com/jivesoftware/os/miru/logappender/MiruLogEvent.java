package com.jivesoftware.os.miru.logappender;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class MiruLogEvent {

    public final String datacenter;
    public final String cluster;
    public final String host;
    public final String service;
    public final String instance;
    public final String version;
    public final String level;
    public final String threadName;
    public final String loggerName;
    public final String message;
    public final String timestamp;
    public final String[] thrownStackTrace;

    @JsonCreator
    public MiruLogEvent(@JsonProperty("datacenter") String datacenter,
        @JsonProperty("cluster") String cluster,
        @JsonProperty("host") String host,
        @JsonProperty("service") String service,
        @JsonProperty("instance") String instance,
        @JsonProperty("version") String version,
        @JsonProperty("level") String level,
        @JsonProperty("threadName") String threadName,
        @JsonProperty("loggerName") String loggerName,
        @JsonProperty("message") String message,
        @JsonProperty("timestamp") String timestamp,
        @JsonProperty("thrownStackTrace") String[] thrownStackTrace) {
        this.datacenter = datacenter;
        this.cluster = cluster;
        this.host = host;
        this.service = service;
        this.instance = instance;
        this.version = version;
        this.level = level;
        this.threadName = threadName;
        this.loggerName = loggerName;
        this.message = message;
        this.timestamp = timestamp;
        this.thrownStackTrace = thrownStackTrace;
    }
}
