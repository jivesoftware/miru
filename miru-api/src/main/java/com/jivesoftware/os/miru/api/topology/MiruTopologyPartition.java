package com.jivesoftware.os.miru.api.topology;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionState;

/**
 *
 */
public class MiruTopologyPartition {

    public MiruHost host;
    public int partitionId;
    public MiruPartitionState state;
    public MiruBackingStorage storage;
    public long destroyAfterTimestamp;

    public MiruTopologyPartition() {
    }

    public MiruTopologyPartition(MiruHost miruHost, int partitionId, MiruPartitionState state, MiruBackingStorage storage, long destroyAfterTimestamp) {
        this.host = miruHost;
        this.partitionId = partitionId;
        this.state = state;
        this.storage = storage;
        this.destroyAfterTimestamp = destroyAfterTimestamp;
    }

}
