package com.jivesoftware.os.miru.wal.activity.hbase;

public class MiruActivityWALColumnKey implements Comparable<MiruActivityWALColumnKey> {
    private final byte sort;
    private final long collisionId;

    public MiruActivityWALColumnKey(byte sort, long collisionId) {
        this.sort = sort;
        this.collisionId = collisionId;
    }

    public byte getSort() {
        return sort;
    }

    public long getCollisionId() {
        return collisionId;
    }

    @Override
    public String toString() {
        return "MiruActivityWALColumnKey{" +
            "sort=" + sort +
            ", collisionId=" + collisionId +
            '}';
    }

    @Override
    public int compareTo(MiruActivityWALColumnKey o) {
        int result = Byte.compare(sort, o.sort);
        if (result == 0) {
            result = Long.compare(collisionId, o.collisionId);
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MiruActivityWALColumnKey that = (MiruActivityWALColumnKey) o;

        if (collisionId != that.collisionId) {
            return false;
        }
        return sort == that.sort;
    }

    @Override
    public int hashCode() {
        int result = sort;
        result = 31 * result + (int) (collisionId ^ (collisionId >>> 32));
        return result;
    }
}
