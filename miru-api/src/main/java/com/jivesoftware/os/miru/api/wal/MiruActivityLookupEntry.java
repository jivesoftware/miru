package com.jivesoftware.os.miru.api.wal;

/**
 *
 */
public class MiruActivityLookupEntry {

    public int partitionId;
    public int index;
    public int writerId;
    public boolean removed;

    public MiruActivityLookupEntry() {
    }

    public MiruActivityLookupEntry(int partitionId, int index, int writerId, boolean removed) {
        this.partitionId = partitionId;
        this.index = index;
        this.writerId = writerId;
        this.removed = removed;
    }
}
