package com.jivesoftware.os.miru.wal.lookup;

/**
 *
 */
public class MiruVersionedActivityLookupEntry {

    public final long version;
    public final MiruActivityLookupEntry entry;

    public MiruVersionedActivityLookupEntry(long version, MiruActivityLookupEntry entry) {
        this.version = version;
        this.entry = entry;
    }
}
