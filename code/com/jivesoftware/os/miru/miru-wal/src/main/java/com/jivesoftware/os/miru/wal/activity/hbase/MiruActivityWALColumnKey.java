package com.jivesoftware.os.miru.wal.activity.hbase;

public class MiruActivityWALColumnKey {
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
        if (sort != that.sort) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) sort;
        result = 31 * result + (int) (collisionId ^ (collisionId >>> 32));
        return result;
    }
}
