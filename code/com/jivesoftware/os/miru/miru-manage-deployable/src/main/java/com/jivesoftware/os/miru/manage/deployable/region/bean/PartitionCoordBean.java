package com.jivesoftware.os.miru.manage.deployable.region.bean;

import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;

/**
 *
 */
public class PartitionCoordBean {

    private final int partitionId;
    private final MiruHost host;
    private final MiruBackingStorage backingStorage;
    private long sizeInMemory;
    private long sizeOnDisk;

    public PartitionCoordBean(MiruPartitionCoord coord, MiruBackingStorage backingStorage, long sizeInMemory, long sizeOnDisk) {
        this.sizeInMemory = sizeInMemory;
        this.sizeOnDisk = sizeOnDisk;
        this.partitionId = coord.partitionId.getId();
        this.host = coord.host;
        this.backingStorage = backingStorage;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public MiruHost getHost() {
        return host;
    }

    public MiruBackingStorage getBackingStorage() {
        return backingStorage;
    }

    public String getSizeInMemory() {
        return readableSize(sizeInMemory);
    }

    public String getSizeOnDisk() {
        return readableSize(sizeOnDisk);
    }

    private String readableSize(long sizeInBytes) {
        if (sizeInBytes > 1_000_000) {
            return (sizeInBytes / 1_000_000) + " MB";
        } else if (sizeInBytes > 1_000) {
            return (sizeInBytes / 1_000) + " kB";
        } else if (sizeInBytes < 0) {
            return "n/a";
        } else {
            return sizeInBytes + " b";
        }
    }
}
