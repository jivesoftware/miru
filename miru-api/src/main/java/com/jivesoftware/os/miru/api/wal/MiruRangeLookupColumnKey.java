package com.jivesoftware.os.miru.api.wal;

/**
 *
 */
public class MiruRangeLookupColumnKey implements Comparable<MiruRangeLookupColumnKey> {

    public final int partitionId;
    public final byte type;

    public MiruRangeLookupColumnKey(int partitionId, byte type) {
        this.partitionId = partitionId;
        this.type = type;
    }

    @Override
    public int compareTo(MiruRangeLookupColumnKey o) {
        int c = Integer.compare(partitionId, o.partitionId);
        if (c == 0) {
            c = Byte.compare(type, o.type);
        }
        return c;
    }
}
