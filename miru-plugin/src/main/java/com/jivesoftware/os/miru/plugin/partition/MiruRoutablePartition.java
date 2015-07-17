package com.jivesoftware.os.miru.plugin.partition;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;

/**
 *
 * @author jonathan.colt
 */
public class MiruRoutablePartition {

    public final MiruHost host;
    public final MiruPartitionId partitionId;
    public final boolean local;
    public final MiruPartitionState state;
    public final MiruBackingStorage storage;
    public final long destroyAfterTimestamp;

    public MiruRoutablePartition(MiruHost host,
        MiruPartitionId partitionId,
        boolean local,
        MiruPartitionState state,
        MiruBackingStorage storage,
        long destroyAfterTimestamp) {
        this.host = host;
        this.partitionId = partitionId;
        this.local = local;
        this.state = state;
        this.storage = storage;
        this.destroyAfterTimestamp = destroyAfterTimestamp;
    }

}
