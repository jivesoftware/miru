package com.jivesoftware.os.miru.plugin.partition;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public class OrderedPartitions<BM> {

    public final MiruTenantId tenantId;
    public final MiruPartitionId partitionId;
    public final Iterable<MiruQueryablePartition<BM>> partitions;

    public OrderedPartitions(MiruTenantId tenantId, MiruPartitionId partitionId, Iterable<MiruQueryablePartition<BM>> partitions) {
        this.tenantId = tenantId;
        this.partitionId = partitionId;
        this.partitions = partitions;
    }
}
