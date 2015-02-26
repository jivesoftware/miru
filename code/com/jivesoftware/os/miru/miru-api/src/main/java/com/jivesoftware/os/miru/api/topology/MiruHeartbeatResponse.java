package com.jivesoftware.os.miru.api.topology;

import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class MiruHeartbeatResponse {

    public List<Partition> active;
    public List<byte[]> topologyHasChanged;

    public MiruHeartbeatResponse() {
    }

    public MiruHeartbeatResponse(List<Partition> active, List<byte[]> topologyHasChanged) {
        this.active = active;
        this.topologyHasChanged = topologyHasChanged;
    }

    static public class Partition {

        public byte[] tenantId;
        public int partitionId;
        public boolean idle;
        public boolean active;

        public Partition() {
        }

        public Partition(byte[] tenantId, int partitionId, boolean idle, boolean active) {
            this.tenantId = tenantId;
            this.partitionId = partitionId;
            this.idle = idle;
            this.active = active;
        }
    }
}
