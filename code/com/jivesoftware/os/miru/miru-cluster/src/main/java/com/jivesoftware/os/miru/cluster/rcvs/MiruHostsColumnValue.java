package com.jivesoftware.os.miru.cluster.rcvs;

/**
 *
 */
public class MiruHostsColumnValue {

    public final long sizeInMemory;
    public final long sizeOnDisk;

    public MiruHostsColumnValue(long sizeInMemory, long sizeOnDisk) {
        this.sizeInMemory = sizeInMemory;
        this.sizeOnDisk = sizeOnDisk;
    }
}
