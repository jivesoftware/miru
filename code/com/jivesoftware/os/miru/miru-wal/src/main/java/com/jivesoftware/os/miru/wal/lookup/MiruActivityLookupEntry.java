package com.jivesoftware.os.miru.wal.lookup;

/**
*
*/
public class MiruActivityLookupEntry {

    public final int partitionId;
    public final int index;
    public final int writerId;
    public final boolean removed;

    public MiruActivityLookupEntry(int partitionId, int index, int writerId, boolean removed) {
        this.partitionId = partitionId;
        this.index = index;
        this.writerId = writerId;
        this.removed = removed;
    }
}
