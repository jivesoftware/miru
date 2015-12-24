package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.partition.MiruRoutablePartition;
import com.jivesoftware.os.miru.service.partition.cluster.PartitionAndHost;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

public class MiruTenantRoutingTopology {

    private final ConcurrentSkipListMap<PartitionAndHost, MiruRoutablePartition> topology;
    private final MiruHostedPartitionComparison partitionComparison;

    public MiruTenantRoutingTopology(MiruHostedPartitionComparison partitionComparison,
        ConcurrentSkipListMap<PartitionAndHost, MiruRoutablePartition> topology) {
        this.partitionComparison = partitionComparison;
        this.topology = topology;
    }

    public List<PartitionGroup> allPartitionsInOrder(MiruTenantId tenantId, String requestName, String queryKey) {
        List<MiruRoutablePartition> allPartitions = Lists.newArrayList(topology.values());

        ListMultimap<MiruPartitionId, MiruRoutablePartition> partitionsPerId = Multimaps.index(allPartitions,
            input -> input.partitionId);

        List<PartitionGroup> allOrderedPartitions = Lists.newArrayList();
        List<MiruPartitionId> partitionIds = Lists.newArrayList(partitionsPerId.keySet());
        Collections.sort(partitionIds);
        Collections.reverse(partitionIds);
        for (MiruPartitionId partitionId : partitionIds) {
            List<MiruRoutablePartition> partitions = partitionsPerId.get(partitionId);
            List<MiruRoutablePartition> orderedPartitions = partitionComparison.orderPartitions(tenantId, partitionId, requestName, queryKey, partitions);
            allOrderedPartitions.add(new PartitionGroup(tenantId, partitionId, orderedPartitions));
        }

        return allOrderedPartitions;
    }

    public static class PartitionGroup {

        public final MiruTenantId tenantId;
        public final MiruPartitionId partitionId;
        public final List<MiruRoutablePartition> partitions;

        public PartitionGroup(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruRoutablePartition> partitions) {
            this.tenantId = tenantId;
            this.partitionId = partitionId;
            this.partitions = partitions;
        }
    }

    @Override
    public String toString() {
        return "MiruTenantRoutingTopology{"
            + ", topology=" + topology.values()
            + '}';
    }

}
