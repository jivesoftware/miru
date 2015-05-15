package com.jivesoftware.os.miru.api.wal;

/**
 *
 */
public class MiruVersionedActivityLookupEntry {

    public long version;
    public MiruActivityLookupEntry entry;

    public MiruVersionedActivityLookupEntry() {
    }

    public MiruVersionedActivityLookupEntry(long version, MiruActivityLookupEntry entry) {
        this.version = version;
        this.entry = entry;
    }
}
