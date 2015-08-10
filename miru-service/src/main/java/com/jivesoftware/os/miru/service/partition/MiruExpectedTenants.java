package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.CoordinateStream;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.context.RequestContextCallback;
import com.jivesoftware.os.miru.plugin.partition.OrderedPartitions;
import com.jivesoftware.os.miru.service.partition.cluster.MiruTenantTopology;

/**
 * To expect a tenant is to consider it active, meaning its partitions are eligible to wake and serve activity.
 * <p>
 * To host a tenant means at least one partition for the tenant is expected to be replicated.
 * <p>
 * The topology for a tenant is the complete breakdown of hosts and partitions.
 */
public interface MiruExpectedTenants {

    MiruTenantTopology<?> getLocalTopology(MiruTenantId tenantId) throws Exception;

    boolean prioritizeRebuild(MiruPartitionCoord coord) throws Exception;

    void thumpthump() throws Exception;

    Iterable<? extends OrderedPartitions<?>> allQueryablePartitionsInOrder(MiruTenantId tenantId, String queryKey) throws Exception;

    boolean expectedTopologies(CoordinateStream stream) throws Exception;
}
