package com.jivesoftware.os.miru.wal.readtracking.rcvs;

import com.google.common.base.Optional;

public class MiruReadTrackingSipWALColumnKey {
    private final long sipId;
    private final Optional<Long> eventId;

    /** Used for reading from RCVS with a ColumnRangeFilter */
    public MiruReadTrackingSipWALColumnKey(long sipId) {
        this.sipId = sipId;
        this.eventId = Optional.absent();
    }

    public MiruReadTrackingSipWALColumnKey(long sipId, long eventId) {
        this.sipId = sipId;
        this.eventId = Optional.of(eventId);
    }

    public long getSipId() {
        return sipId;
    }

    public Optional<Long> getEventId() {
        return eventId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MiruReadTrackingSipWALColumnKey that = (MiruReadTrackingSipWALColumnKey) o;

        if (sipId != that.sipId) {
            return false;
        }
        return !(eventId != null ? !eventId.equals(that.eventId) : that.eventId != null);
    }

    @Override
    public int hashCode() {
        int result = (int) (sipId ^ (sipId >>> 32));
        result = 31 * result + (eventId != null ? eventId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MiruReadTrackingSipWALColumnKey{" +
            "sipId=" + sipId +
            ", eventId=" + eventId +
            '}';
    }
}
