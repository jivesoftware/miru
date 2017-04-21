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
    private final String lastIngress;
    private final String lastQuery;
    private final String lastTimestamp;
    private final String lastTimestampTimeAgo;

    public PartitionCoordBean(MiruPartitionCoord coord,
        MiruBackingStorage backingStorage,
        String lastIngress,
        String lastQuery,
        String lastTimestamp,
        String lastTimestampTimeAgo) {
        this.partitionId = coord.partitionId.getId();
        this.host = coord.host;
        this.backingStorage = backingStorage;
        this.lastIngress = lastIngress;
        this.lastQuery = lastQuery;
        this.lastTimestamp = lastTimestamp;
        this.lastTimestampTimeAgo = lastTimestampTimeAgo;
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

    public String getLastIngress() {
        return lastIngress;
    }

    public String getLastQuery() {
        return lastQuery;
    }

    public String getLastTimestamp() {
        return lastTimestamp;
    }

    public String getLastTimestampTimeAgo() {
        return lastTimestampTimeAgo;
    }
}
