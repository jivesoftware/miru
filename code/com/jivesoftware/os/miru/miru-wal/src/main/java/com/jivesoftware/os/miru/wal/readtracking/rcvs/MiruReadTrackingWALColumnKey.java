package com.jivesoftware.os.miru.wal.readtracking.rcvs;

public class MiruReadTrackingWALColumnKey {
    private final long eventId;

    public MiruReadTrackingWALColumnKey(long eventId) {
        this.eventId = eventId;
    }

    public long getEventId() {
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

        MiruReadTrackingWALColumnKey that = (MiruReadTrackingWALColumnKey) o;

        return eventId == that.eventId;
    }

    @Override
    public int hashCode() {
        return (int) (eventId ^ (eventId >>> 32));
    }

    @Override
    public String toString() {
        return "MiruReadTrackingWALColumnKey{" +
            "eventId=" + eventId +
            '}';
    }
}
