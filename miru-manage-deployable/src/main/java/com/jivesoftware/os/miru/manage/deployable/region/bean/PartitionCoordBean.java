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
    private final String lastId;
    private final String lastIdTimeAgo;

    public PartitionCoordBean(MiruPartitionCoord coord,
        MiruBackingStorage backingStorage,
        String lastIngress,
        String lastQuery,
        String lastId,
        String lastIdTimeAgo) {
        this.partitionId = coord.partitionId.getId();
        this.host = coord.host;
        this.backingStorage = backingStorage;
        this.lastIngress = lastIngress;
        this.lastQuery = lastQuery;
        this.lastId = lastId;
        this.lastIdTimeAgo = lastIdTimeAgo;
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

    public String getLastId() {
        return lastId;
    }

    public String getLastIdTimeAgo() {
        return lastIdTimeAgo;
    }
}
