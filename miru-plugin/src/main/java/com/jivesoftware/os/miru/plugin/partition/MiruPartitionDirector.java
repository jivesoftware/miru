package com.jivesoftware.os.miru.plugin.partition;

import com.google.common.base.Optional;
import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.activity.CoordinateStream;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;

/**
 *
 */
public interface MiruPartitionDirector {

    Optional<? extends MiruQueryablePartition<?, ?>> getQueryablePartition(MiruPartitionCoord miruPartitionCoord) throws Exception;

    void index(ListMultimap<MiruTenantId, MiruPartitionedActivity> perTenantPartitionedActivities) throws Exception;

    Iterable<? extends OrderedPartitions<?, ?>> allQueryablePartitionsInOrder(MiruTenantId tenantId, String requestName, String queryKey) throws Exception;

    OrderedPartitions<?, ?> queryablePartitionInOrder(MiruTenantId tenantId, MiruPartitionId partitionId, String requestName, String queryKey) throws Exception;

    void warm(MiruTenantId tenantId) throws Exception;

    boolean checkInfo(MiruTenantId tenantId, MiruPartitionId partitionId, MiruPartitionCoordInfo info) throws Exception;

    MiruPartitionCoordInfo getInfo(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    boolean prioritizeRebuild(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    boolean compact(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    boolean rebuildTimeRange(MiruTimeRange miruTimeRange, boolean hotDeploy, boolean chunkStores, boolean labIndex) throws Exception;

    boolean expectedTopologies(Optional<MiruTenantId> tenantId, CoordinateStream stream) throws Exception;

}
