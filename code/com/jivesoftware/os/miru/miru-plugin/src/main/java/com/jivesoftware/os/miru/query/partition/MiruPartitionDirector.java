package com.jivesoftware.os.miru.query.partition;

import com.google.common.base.Optional;
import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public interface MiruPartitionDirector {

    Optional<MiruHostedPartition<?>> getQueryablePartition(MiruPartitionCoord miruPartitionCoord);

    void index(ListMultimap<MiruTenantId, MiruPartitionedActivity> perTenantPartitionedActivities) throws Exception;

    Iterable<OrderedPartitions> allQueryablePartitionsInOrder(MiruTenantId tenantId);

    void warm(MiruTenantId tenantId) throws Exception;

    void setStorage(MiruTenantId tenantId, MiruPartitionId partitionId, MiruBackingStorage storage) throws Exception;

    void removeHost(MiruHost host) throws Exception;

    void removeReplicas(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    void moveReplica(MiruTenantId tenantId, MiruPartitionId partitionId, Optional<MiruHost> fromHost, MiruHost localhost) throws Exception;

    void removeTopology(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) throws Exception;

    boolean checkInfo(MiruTenantId tenantId, MiruPartitionId partitionId, MiruPartitionCoordInfo info);
}
