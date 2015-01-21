package com.jivesoftware.os.miru.wal.readtracking.hbase;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import java.util.Comparator;

public class MiruReadTrackingWALRow implements Comparable<MiruReadTrackingWALRow> {

    private static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();

    private final MiruStreamId streamId;

    public MiruReadTrackingWALRow(MiruStreamId streamId) {
        this.streamId = streamId;
    }

    public MiruStreamId getStreamId() {
        return streamId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MiruReadTrackingWALRow that = (MiruReadTrackingWALRow) o;

        return !(streamId != null ? !streamId.equals(that.streamId) : that.streamId != null);
    }

    @Override
    public int hashCode() {
        return streamId != null ? streamId.hashCode() : 0;
    }

    @Override
    public int compareTo(MiruReadTrackingWALRow o) {
        return COMPARATOR.compare(streamId.immutableBytes(), o.streamId.immutableBytes());
    }

    @Override
    public String toString() {
        return "MiruReadTrackingWALRow{" +
            "streamId=" + streamId +
            '}';
    }
}