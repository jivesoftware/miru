package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;

/**
 *
 */
public class OrderedPartitions<M extends MiruHostedPartition> {

    public final MiruTenantId tenantId;
    public final MiruPartitionId partitionId;
    public final List<M> partitions;

    public OrderedPartitions(MiruTenantId tenantId, MiruPartitionId partitionId, List<M> partitions) {
        this.tenantId = tenantId;
        this.partitionId = partitionId;
        this.partitions = partitions;
    }
}
