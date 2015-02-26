package com.jivesoftware.os.miru.api.topology;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import java.util.List;

/**
 *
 * @author jonathan.colt
 */
public class MiruHeartbeatRequest {

    public List<Partition> active;

    public MiruHeartbeatRequest() {
    }

    public MiruHeartbeatRequest(List<Partition> active) {
        this.active = active;
    }

    static public class Partition {

        public byte[] tenantId;
        public int partitionId;
        public long activeTimestamp; // -1 means unchanges
        public MiruPartitionState state;
        public MiruBackingStorage storage;

        public Partition() {
        }

        public Partition(byte[] tenantId, int partitionId, long activeTimestamp, MiruPartitionState state, MiruBackingStorage storage) {
            this.tenantId = tenantId;
            this.partitionId = partitionId;
            this.activeTimestamp = activeTimestamp;
            this.state = state;
            this.storage = storage;
        }

    }
}
