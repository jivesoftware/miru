package com.jivesoftware.os.miru.writer.deployable.region.bean;

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
    private final String idle;

    public PartitionCoordBean(MiruPartitionCoord coord, MiruBackingStorage backingStorage, String idle) {
        this.idle = idle;
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

    public String getIdle() {
        return idle;
    }
}
