package com.jivesoftware.os.miru.api.topology;

import java.util.Collection;

/**
 * @author jonathan.colt
 */
public class MiruHeartbeatResponse {

    public NamedCursorsResult<Collection<MiruPartitionActiveUpdate>> activeHasChanged;
    public NamedCursorsResult<Collection<MiruTenantTopologyUpdate>> topologyHasChanged;

    public MiruHeartbeatResponse() {
    }

    public MiruHeartbeatResponse(NamedCursorsResult<Collection<MiruPartitionActiveUpdate>> activeHasChanged,
        NamedCursorsResult<Collection<MiruTenantTopologyUpdate>> topologyHasChanged) {
        this.activeHasChanged = activeHasChanged;
        this.topologyHasChanged = topologyHasChanged;
    }

}
