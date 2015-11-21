package com.jivesoftware.os.miru.plugin.partition;

import com.google.common.base.Optional;
import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.activity.CoordinateStream;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public interface MiruPartitionDirector {

    Optional<? extends MiruQueryablePartition<?, ?>> getQueryablePartition(MiruPartitionCoord miruPartitionCoord) throws Exception;

    void index(ListMultimap<MiruTenantId, MiruPartitionedActivity> perTenantPartitionedActivities) throws Exception;

    Iterable<? extends OrderedPartitions<?, ?>> allQueryablePartitionsInOrder(MiruTenantId tenantId, String queryKey) throws Exception;

    void warm(MiruTenantId tenantId) throws Exception;

    void setStorage(MiruTenantId tenantId, MiruPartitionId partitionId, MiruBackingStorage storage) throws Exception;

    void removeHost(MiruHost host) throws Exception;

    void removeTopology(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) throws Exception;

    boolean checkInfo(MiruTenantId tenantId, MiruPartitionId partitionId, MiruPartitionCoordInfo info) throws Exception;

    boolean prioritizeRebuild(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    boolean expectedTopologies(Optional<MiruTenantId> tenantId, CoordinateStream stream) throws Exception;
}
