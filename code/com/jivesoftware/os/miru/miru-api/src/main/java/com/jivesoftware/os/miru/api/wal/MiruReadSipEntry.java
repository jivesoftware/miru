package com.jivesoftware.os.miru.api.wal;

/**
 *
 * @author jonathan.colt
 */
public class MiruReadSipEntry {

    public long eventId;
    public long timestamp;

    public MiruReadSipEntry() {
    }

    public MiruReadSipEntry(long eventId, long timestamp) {
        this.eventId = eventId;
        this.timestamp = timestamp;
    }

}
