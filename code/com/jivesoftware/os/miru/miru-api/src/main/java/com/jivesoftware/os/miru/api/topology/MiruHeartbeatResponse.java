package com.jivesoftware.os.miru.api.topology;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class MiruHeartbeatResponse {

    public List<Partition> active;
    public List<MiruTenantTopologyUpdate> topologyHasChanged;

    public MiruHeartbeatResponse() {
    }

    public MiruHeartbeatResponse(List<Partition> active, List<MiruTenantTopologyUpdate> topologyHasChanged) {
        this.active = active;
        this.topologyHasChanged = topologyHasChanged;
    }

    static public class Partition {

        public MiruTenantId tenantId;
        public int partitionId;
        public boolean active;
        public boolean idle;

        public Partition() {
        }

        public Partition(MiruTenantId tenantId, int partitionId, boolean active, boolean idle) {
            this.tenantId = tenantId;
            this.partitionId = partitionId;
            this.active = active;
            this.idle = idle;
        }
    }
}
