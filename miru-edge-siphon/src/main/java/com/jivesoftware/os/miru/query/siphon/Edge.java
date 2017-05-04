package com.jivesoftware.os.miru.query.siphon;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Set;

/**
 * Created by jonathan.colt on 5/1/17.
 */
public class Edge implements Serializable {

    public final long id;
    public final long timestamp;
    public final String tenant;
    public final String user;
    public final String name;
    public final String origin;
    public final String destination;
    public final Set<String> tags;
    public final long latency;

    @JsonCreator
    public Edge(@JsonProperty("id") long id,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("tenant") String tenant,
        @JsonProperty("user") String user,
        @JsonProperty("name") String name,
        @JsonProperty("origin") String origin,
        @JsonProperty("destination") String destination,
        @JsonProperty("tags") Set<String> tags,
        @JsonProperty("latency") long latency) {

        this.id = id;
        this.timestamp = timestamp;
        this.tenant = tenant;
        this.user = user;
        this.name = name;
        this.origin = origin;
        this.destination = destination;
        this.tags = tags;
        this.latency = latency;
    }
}
