package com.jivesoftware.os.miru.wal.readtracking.hbase;

import com.jivesoftware.os.miru.api.base.MiruStreamId;

public class MiruReadTrackingWALRow {
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

        if (streamId != null ? !streamId.equals(that.streamId) : that.streamId != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return streamId != null ? streamId.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "MiruReadTrackingWALRow{" +
            "streamId=" + streamId +
            '}';
    }
}