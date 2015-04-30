package com.jivesoftware.os.miru.api.topology;

import java.util.Collection;

/**
 *
 * @author jonathan.colt
 */
public class MiruHeartbeatRequest {

    public Collection<PartitionInfo> active;
    public Collection<NamedCursor> partitionActiveUpdatesSinceCursors;
    public Collection<NamedCursor> topologyUpdatesSinceCursors;

    public MiruHeartbeatRequest() {
    }

    public MiruHeartbeatRequest(Collection<PartitionInfo> active,
        Collection<NamedCursor> partitionActiveUpdatesSinceCursors,
        Collection<NamedCursor> topologyUpdatesSinceCursors) {
        this.active = active;
        this.partitionActiveUpdatesSinceCursors = partitionActiveUpdatesSinceCursors;
        this.topologyUpdatesSinceCursors = topologyUpdatesSinceCursors;
    }

}
