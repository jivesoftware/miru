package com.jivesoftware.os.miru.cluster.rcvs;

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
