package com.jivesoftware.os.miru.wal.activity.hbase;

import com.google.common.base.Optional;

public class MiruActivitySipWALColumnKey implements Comparable<MiruActivitySipWALColumnKey> {
    private final byte sort;
    private final long collisionId;
    private final Optional<Long> sipId;

    public MiruActivitySipWALColumnKey(byte sort, long collisionId, long sipId) {
        this.sort = sort;
        this.collisionId = collisionId;
        this.sipId = Optional.of(sipId);
    }

    /** Only used for reading from the sip WAL */
    public MiruActivitySipWALColumnKey(byte sort, long collisionId) {
        this.sort = sort;
        this.collisionId = collisionId;
        this.sipId = Optional.absent();
    }

    public byte getSort() {
        return sort;
    }

    public long getCollisionId() {
        return collisionId;
    }

    public Optional<Long> getSipId() {
        return sipId;
    }

    @Override
    public int compareTo(MiruActivitySipWALColumnKey o) {
        int result = Byte.compare(sort, o.sort);
        if (result == 0) {
            result = Long.compare(collisionId, o.collisionId);
        }
        if (result == 0) {
            long a = sipId.or(Long.MAX_VALUE);
            long b = o.sipId.or(Long.MAX_VALUE);
            result = Long.compare(a, b);
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
        if (sort != that.sort) {
            return false;
        }
        return !(sipId != null ? !sipId.equals(that.sipId) : that.sipId != null);
    }

    @Override
    public int hashCode() {
        int result = sort;
        result = 31 * result + (int) (collisionId ^ (collisionId >>> 32));
        result = 31 * result + (sipId != null ? sipId.hashCode() : 0);
        return result;
    }
}
