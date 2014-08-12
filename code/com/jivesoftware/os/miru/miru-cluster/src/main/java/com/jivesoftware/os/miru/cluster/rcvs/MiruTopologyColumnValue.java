package com.jivesoftware.os.miru.cluster.rcvs;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionState;

/**
*
*/
public class MiruTopologyColumnValue {

    public final MiruPartitionState state;
    public final MiruBackingStorage storage;
    public final long sizeInMemory;
    public final long sizeOnDisk;

    public MiruTopologyColumnValue(MiruPartitionState state, MiruBackingStorage storage, long sizeInMemory, long sizeOnDisk) {
        this.state = state;
        this.storage = storage;
        this.sizeInMemory = sizeInMemory;
        this.sizeOnDisk = sizeOnDisk;
    }
}
