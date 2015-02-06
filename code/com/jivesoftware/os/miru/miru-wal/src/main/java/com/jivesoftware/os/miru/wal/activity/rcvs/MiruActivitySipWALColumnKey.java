package com.jivesoftware.os.miru.wal.activity.rcvs;

public class MiruActivitySipWALColumnKey implements Comparable<MiruActivitySipWALColumnKey> {

    private final byte sort;
    private final long collisionId;
    private final long sipId;

    public MiruActivitySipWALColumnKey(byte sort, long collisionId, long sipId) {
        this.sort = sort;
        this.collisionId = collisionId;
        this.sipId = sipId;
    }

    public byte getSort() {
        return sort;
    }

    public long getCollisionId() {
        return collisionId;
    }

    public long getSipId() {
        return sipId;
    }

    @Override
    public int compareTo(MiruActivitySipWALColumnKey o) {
        int result = Byte.compare(sort, o.sort);
        if (result == 0) {
            result = Long.compare(collisionId, o.collisionId);
        }
        if (result == 0) {
            result = Long.compare(sipId, o.sipId);
        }
        return result;
    }

    @Override
    public String toString() {
        return "MiruActivitySipWALColumnKey{" +
            "sort=" + sort +
            ", collisionId=" + collisionId +
            ", sipId=" + sipId +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MiruActivitySipWALColumnKey that = (MiruActivitySipWALColumnKey) o;

        if (collisionId != that.collisionId) {
            return false;
        }
        if (sipId != that.sipId) {
            return false;
        }
        if (sort != that.sort) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) sort;
        result = 31 * result + (int) (collisionId ^ (collisionId >>> 32));
        result = 31 * result + (int) (sipId ^ (sipId >>> 32));
        return result;
    }
}
