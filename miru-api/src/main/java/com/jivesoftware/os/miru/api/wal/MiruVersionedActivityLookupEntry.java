package com.jivesoftware.os.miru.api.wal;

/**
 *
 */
public class MiruVersionedActivityLookupEntry {

    public long timestamp;
    public long version;
    public MiruActivityLookupEntry entry;

    public MiruVersionedActivityLookupEntry() {
    }

    public MiruVersionedActivityLookupEntry(long timestamp, long version, MiruActivityLookupEntry entry) {
        this.timestamp = timestamp;
        this.version = version;
        this.entry = entry;
    }
}
