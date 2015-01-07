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

    public PartitionCoordBean(MiruPartitionCoord coord, MiruBackingStorage backingStorage) {
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

}
