package com.jivesoftware.os.miru.query.siphon;

import java.util.Set;

/**
 * Created by jonathan.colt on 5/1/17.
 */
public class Edge {

    public long id;
    public long timestamp;
    public String tenant;
    public String name;
    public String origin;
    public String destination;
    public Set<String> tags;
    public long latency;
}
