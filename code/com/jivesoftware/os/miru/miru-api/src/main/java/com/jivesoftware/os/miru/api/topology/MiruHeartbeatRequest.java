package com.jivesoftware.os.miru.api.topology;

import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class MiruHeartbeatRequest {

    public List<Partition> active;
    public long topologyUpdatesSinceTimestamp;

    public MiruHeartbeatRequest() {
    }

    public MiruHeartbeatRequest(List<Partition> active, long topologyUpdatesSinceTimestamp) {
        this.active = active;
        this.topologyUpdatesSinceTimestamp = topologyUpdatesSinceTimestamp;
    }

    static public class Partition {

        public MiruTenantId tenantId;
        public int partitionId;
        public long activeTimestamp; // -1 means unchanged
        public MiruPartitionCoordInfo info;

        public Partition() {
        }

        public Partition(MiruTenantId tenantId, int partitionId, long activeTimestamp, MiruPartitionCoordInfo info) {
            this.tenantId = tenantId;
            this.partitionId = partitionId;
            this.activeTimestamp = activeTimestamp;
            this.info = info;
        }

    }
}
