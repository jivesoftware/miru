package com.jivesoftware.os.miru.manage.deployable;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 *
 */
public class MiruRebalanceDirector {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruClusterRegistry clusterRegistry;
    private final OrderIdProvider orderIdProvider;
    private final Random random = new Random();

    public MiruRebalanceDirector(MiruClusterRegistry clusterRegistry, OrderIdProvider orderIdProvider) {
        this.clusterRegistry = clusterRegistry;
        this.orderIdProvider = orderIdProvider;
    }

    public void shiftTopologies(MiruHost fromHost, float probability, boolean currentPartitionOnly) throws Exception {
        List<MiruHost> allHosts = Lists.newArrayList(Collections2.transform(clusterRegistry.getAllHosts(),
            new Function<MiruClusterRegistry.HostHeartbeat, MiruHost>() {
                @Override
                public MiruHost apply(MiruClusterRegistry.HostHeartbeat input) {
                    return input.host;
                }
            }));

        for (MiruTenantId tenantId : clusterRegistry.getTenantsForHost(fromHost)) {
            int numberOfReplicas = clusterRegistry.getNumberOfReplicas(tenantId);
            ListMultimap<MiruPartitionState, MiruPartition> partitionsForTenant = clusterRegistry.getPartitionsForTenant(tenantId);
            MiruPartitionId currentPartitionId = findCurrentPartitionId(partitionsForTenant);
            Table<MiruTenantId, MiruPartitionId, List<MiruPartition>> replicaTable = extractPartitions(
                currentPartitionOnly, tenantId, partitionsForTenant, currentPartitionId);
            for (Table.Cell<MiruTenantId, MiruPartitionId, List<MiruPartition>> cell : replicaTable.cellSet()) {
                if (random.nextFloat() > probability) {
                    LOG.inc("rebalance>skipped");
                    continue;
                }
                List<MiruHost> hostsToElect = selectHostsToElect(fromHost, allHosts, numberOfReplicas, cell.getValue());
                electHosts(tenantId, cell.getColumnKey(), hostsToElect);
                LOG.inc("rebalance>moved");
            }
        }
    }

    private List<MiruHost> selectHostsToElect(MiruHost fromHost, List<MiruHost> allHosts, int numberOfReplicas, List<MiruPartition> partitions) {
        int numHosts = allHosts.size();
        List<MiruHost> hostsToElect = Lists.newArrayListWithCapacity(numberOfReplicas);
        if (partitions.isEmpty()) {
            for (int index = allHosts.indexOf(fromHost), j = 0; j < numberOfReplicas && j < numHosts; index++, j++) {
                MiruHost hostToElect = allHosts.get(index % numHosts);
                LOG.debug("Empty {}", hostToElect);
                hostsToElect.add(hostToElect);
            }
        } else {
            Set<MiruHost> hostsWithReplica = Sets.newHashSet();
            List<Integer> hostIndexes = Lists.newArrayListWithCapacity(hostsWithReplica.size());
            for (MiruPartition partition : partitions) {
                hostsWithReplica.add(partition.coord.host);
                hostIndexes.add(allHosts.indexOf(partition.coord.host));
            }
            Collections.sort(hostIndexes);
            int start = startOfContiguousRun(numHosts, hostIndexes);

            boolean contiguous = (start >= 0);
            if (contiguous) {
                //    \_/-.--.--.--.--.--.
                //    (")__)__)__)__)__)__)
                //     ^ "" "" "" "" "" ""
                // magical caterpillar to the right, since walking from next neighbor might cycle
                // e.g. hosts=0-9, partitions=0,8,9, shift(0)=8,9,0
                // instead we do shift(0)=9,0,1
                for (int index = hostIndexes.get(start) + 1, j = 0; j < numberOfReplicas && j < numHosts; index++, j++) {
                    MiruHost hostToElect = allHosts.get(index % numHosts);
                    LOG.debug("Caterpillar {}", hostToElect);
                    hostsToElect.add(hostToElect);
                }
            } else {
                // safe to walk from next neighbor
                int neighborIndex = -1;
                for (int index = allHosts.indexOf(fromHost) + 1, j = 0; j < numHosts; index++, j++) {
                    if (hostIndexes.contains(index % numHosts)) {
                        neighborIndex = index;
                        break;
                    }
                }
                //TODO consider adding isOnline check
                for (int index = neighborIndex, j = 0; j < numberOfReplicas && j < numHosts; index++, j++) {
                    MiruHost hostToElect = allHosts.get(index % numHosts);
                    LOG.debug("Neighbor {}", hostToElect);
                    hostsToElect.add(hostToElect);
                }
            }
        }
        return hostsToElect;
    }

    private void electHosts(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruHost> hostsToElect) throws Exception {
        LOG.debug("Elect {} to {} {}", hostsToElect, tenantId, partitionId);
        for (MiruHost hostToElect : hostsToElect) {
            clusterRegistry.ensurePartitionCoord(new MiruPartitionCoord(tenantId, partitionId, hostToElect));
            clusterRegistry.addToReplicaRegistry(tenantId, partitionId, Long.MAX_VALUE - orderIdProvider.nextId(), hostToElect);
        }
        LOG.inc("rebalance>elect", hostsToElect.size());
    }

    private Table<MiruTenantId, MiruPartitionId, List<MiruPartition>> extractPartitions(boolean currentPartitionOnly,
        MiruTenantId tenantId,
        ListMultimap<MiruPartitionState, MiruPartition> partitionsForTenant,
        MiruPartitionId currentPartitionId) {

        Table<MiruTenantId, MiruPartitionId, List<MiruPartition>> replicaTable = HashBasedTable.create();
        for (MiruPartition partition : partitionsForTenant.values()) {
            MiruPartitionId partitionId = partition.coord.partitionId;
            if (currentPartitionOnly && currentPartitionId != null && partitionId.compareTo(currentPartitionId) < 0) {
                continue;
            }
            List<MiruPartition> partitions = replicaTable.get(tenantId, partitionId);
            if (partitions == null) {
                partitions = Lists.newArrayList();
                replicaTable.put(tenantId, partitionId, partitions);
            }
            if (currentPartitionId == null || partitionId.compareTo(currentPartitionId) > 0) {
                currentPartitionId = partitionId;
            }
            partitions.add(partition);
        }
        return replicaTable;
    }

    private MiruPartitionId findCurrentPartitionId(ListMultimap<MiruPartitionState, MiruPartition> partitionsForTenant) {
        MiruPartitionId currentPartitionId = null;
        for (MiruPartition partition : partitionsForTenant.values()) {
            MiruPartitionId partitionId = partition.coord.partitionId;
            if (currentPartitionId == null || partitionId.compareTo(currentPartitionId) > 0) {
                currentPartitionId = partitionId;
            }
        }
        return currentPartitionId;
    }

    // example:
    //   0. 1. 2.
    // [ 0, 8, 9 ]
    protected int startOfContiguousRun(int numHosts, List<Integer> hostIndexes) {
        // i=0. index=0, contains(9)? -> yes
        // i=1. index=8, contains(7)? -> no. start=1
        int start = -1;
        for (int i = 0; i < hostIndexes.size(); i++) {
            int index = hostIndexes.get(i);
            if (!hostIndexes.contains((index + numHosts - 1) % numHosts)) {
                start = i;
                break;
            }
        }

        // start=1
        // index=8, j=0<3. contains(8)? -> yes
        // index=9, j=1<3. contains(9)? -> yes
        // index=10, j=2<3. contains(0)? -> yes
        if (start >= 0) {
            for (int index = hostIndexes.get(start), j = 0; j < hostIndexes.size(); index++, j++) {
                if (!hostIndexes.contains(index % numHosts)) {
                    start = -1;
                    break;
                }
            }
        }
        return start;
    }
}
