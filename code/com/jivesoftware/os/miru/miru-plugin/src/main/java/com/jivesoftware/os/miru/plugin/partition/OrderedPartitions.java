package com.jivesoftware.os.miru.plugin.partition;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;

/**
 *
 */
public class OrderedPartitions {

    public final MiruTenantId tenantId;
    public final MiruPartitionId partitionId;
    public final List<MiruHostedPartition<?>> partitions;

    public OrderedPartitions(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruHostedPartition<?>> partitions) {
        this.tenantId = tenantId;
        this.partitionId = partitionId;
        this.partitions = partitions;
    }
}
