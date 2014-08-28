package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionCoordMetrics;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;

/**
 *
 */
public class MiruPartitionEventHandler {

    private final MiruClusterRegistry clusterRegistry;

    public MiruPartitionEventHandler(MiruClusterRegistry clusterRegistry) {
        this.clusterRegistry = clusterRegistry;
    }

    public void partitionChanged(MiruPartitionCoord coord, MiruPartitionCoordInfo info, MiruPartitionCoordMetrics metrics, Optional<Long> refreshTimestamp)
        throws Exception {
        clusterRegistry.updateTopology(coord, info, metrics, refreshTimestamp);
    }

    public void refreshTopology(MiruPartitionCoord coord, MiruPartitionCoordMetrics metrics, long refreshTimestamp) throws Exception {
        clusterRegistry.refreshTopology(coord, metrics, refreshTimestamp);
    }

    public boolean isCoordActive(MiruPartitionCoord coord) throws Exception {
        return clusterRegistry.isPartitionActive(coord);
    }
}
