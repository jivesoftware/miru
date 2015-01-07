package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;

/**
 *
 */
public class MiruPartitionEventHandler {

    private final MiruClusterRegistry clusterRegistry;

    public MiruPartitionEventHandler(MiruClusterRegistry clusterRegistry) {
        this.clusterRegistry = clusterRegistry;
    }

    public void updateTopology(MiruPartitionCoord coord,
        Optional<MiruPartitionCoordInfo> info,
        Optional<Long> refreshTimestamp)
        throws Exception {
        clusterRegistry.updateTopology(coord, info, refreshTimestamp);
    }

    public boolean isCoordActive(MiruPartitionCoord coord) throws Exception {
        return clusterRegistry.isPartitionActive(coord);
    }
}
