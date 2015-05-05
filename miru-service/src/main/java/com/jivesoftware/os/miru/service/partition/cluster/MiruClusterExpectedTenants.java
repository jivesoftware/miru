package com.jivesoftware.os.miru.service.partition.cluster;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatResponse;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActiveUpdate;
import com.jivesoftware.os.miru.api.topology.MiruTenantTopologyUpdate;
import com.jivesoftware.os.miru.api.topology.MiruTopologyResponse;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.miru.plugin.partition.MiruRoutablePartition;
import com.jivesoftware.os.miru.plugin.partition.OrderedPartitions;
import com.jivesoftware.os.miru.service.partition.MiruExpectedTenants;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartitionComparison;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruPartitionHeartbeatHandler;
import com.jivesoftware.os.miru.service.partition.MiruRemoteQueryablePartitionFactory;
import com.jivesoftware.os.miru.service.partition.MiruTenantRoutingTopology;
import com.jivesoftware.os.miru.service.partition.MiruTenantRoutingTopology.PartitionGroup;
import com.jivesoftware.os.miru.service.partition.MiruTenantTopologyFactory;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MiruClusterExpectedTenants implements MiruExpectedTenants {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruHost localHost;
    private final MiruTenantTopologyFactory tenantTopologyFactory;
    private final MiruRemoteQueryablePartitionFactory remotePartitionFactory;
    private final MiruPartitionHeartbeatHandler heartbeatHandler;
    private final MiruHostedPartitionComparison partitionComparison;
    private final MiruClusterClient clusterClient;

    private final ConcurrentMap<MiruTenantId, MiruTenantTopology<?>> localTopologies = Maps.newConcurrentMap();
    private final StripingLocksProvider<MiruTenantId> tenantLocks = new StripingLocksProvider<>(64);

    private final Cache<MiruTenantId, MiruTenantRoutingTopology> routingTopologies = CacheBuilder.newBuilder()
        .concurrencyLevel(24)
        .expireAfterAccess(1, TimeUnit.HOURS)
        .maximumSize(10_000) //TODO configure
        .build();

    public MiruClusterExpectedTenants(MiruHost localHost,
        MiruTenantTopologyFactory tenantTopologyFactory,
        MiruRemoteQueryablePartitionFactory remotePartitionFactory,
        MiruPartitionHeartbeatHandler heartbeatHandler,
        MiruHostedPartitionComparison partitionComparison,
        MiruClusterClient clusterClient) {
        this.localHost = localHost;

        this.tenantTopologyFactory = tenantTopologyFactory;
        this.remotePartitionFactory = remotePartitionFactory;
        this.heartbeatHandler = heartbeatHandler;
        this.partitionComparison = partitionComparison;
        this.clusterClient = clusterClient;
    }

    @Override
    public Iterable<? extends OrderedPartitions<?>> allQueryablePartitionsInOrder(final MiruTenantId tenantId,
        String queryKey) throws Exception {

        MiruTenantRoutingTopology topology = routingTopologies.get(tenantId, () -> {

            MiruTopologyResponse topologyResponse = clusterClient.routingTopology(tenantId);
            ConcurrentSkipListMap<PartitionAndHost, MiruRoutablePartition> topology1 = new ConcurrentSkipListMap<PartitionAndHost, MiruRoutablePartition>();
            for (MiruTopologyResponse.Partition partition : topologyResponse.topology) {
                MiruPartitionId partitionId = MiruPartitionId.of(partition.partitionId);
                MiruRoutablePartition routablePartition = new MiruRoutablePartition(partition.host,
                    partitionId, partition.host.equals(localHost),
                    partition.state, partition.storage);
                topology1.put(new PartitionAndHost(partitionId, partition.host), routablePartition);
            }
            return new MiruTenantRoutingTopology(partitionComparison, topology1);
        });
        if (topology == null) {
            return Collections.emptyList();
        }
        final MiruTenantTopology<?> localTopology = localTopologies.get(tenantId);
        return getOrderedPartitions(tenantId, queryKey, topology, localTopology);
    }

    private <BM> List<OrderedPartitions<BM>> getOrderedPartitions(MiruTenantId tenantId,
        String queryKey,
        MiruTenantRoutingTopology topology,
        MiruTenantTopology<BM> localTopology) {

        List<PartitionGroup> allPartitionsInOrder = topology.allPartitionsInOrder(tenantId, queryKey);
        return Lists.transform(allPartitionsInOrder, input -> {
            List<MiruQueryablePartition<BM>> partitions = Lists.transform(input.partitions,
                routablePartition -> {
                    MiruPartitionCoord key = new MiruPartitionCoord(tenantId, routablePartition.partitionId, routablePartition.host);
                    if (routablePartition.local) {
                        if (localTopology == null) {
                            return null;
                        } else {
                            Optional<MiruLocalHostedPartition<BM>> partition = localTopology.getPartition(routablePartition.partitionId);
                            return partition.orNull();
                        }
                    } else {
                        return remotePartitionFactory.create(key, new MiruPartitionCoordInfo(routablePartition.state, routablePartition.storage));
                    }
                });
            return new OrderedPartitions<>(input.tenantId, input.partitionId, Iterables.filter(partitions, Predicates.notNull()));
        });
    }

    @Override
    public void thumpthump() throws Exception {
        MiruHeartbeatResponse thumpthump = heartbeatHandler.thumpthump(localHost);

        ListMultimap<MiruTenantId, MiruPartitionActiveUpdate> tenantPartitionActive = ArrayListMultimap.create();
        for (MiruPartitionActiveUpdate active : thumpthump.activeHasChanged.result) {
            tenantPartitionActive.put(active.tenantId, active);
        }
        expect(tenantPartitionActive);

        for (MiruTenantTopologyUpdate update : thumpthump.topologyHasChanged.result) {
            routingTopologies.invalidate(update.tenantId);
        }
    }

    private void expect(ListMultimap<MiruTenantId, MiruPartitionActiveUpdate> expected) throws Exception {

        for (MiruTenantId tenantId : expected.keys()) {
            synchronized (tenantLocks.lock(tenantId)) {
                MiruTenantTopology<?> tenantTopology = localTopologies.get(tenantId);
                if (tenantTopology == null) {
                    tenantTopology = tenantTopologyFactory.create(tenantId);
                    localTopologies.put(tenantId, tenantTopology);
                }

                tenantTopology.checkForPartitionAlignment(expected.get(tenantId));

                if (tenantTopology.allPartitions().isEmpty()) {
                    final MiruTenantTopology removed = localTopologies.remove(tenantId);
                    if (removed != null) {
                        removed.remove();
                    }
                }
            }
        }
    }

    @Override
    public MiruTenantTopology<?> getLocalTopology(final MiruTenantId tenantId) throws Exception {
        return localTopologies.get(tenantId);
    }

    @Override
    public boolean prioritizeRebuild(MiruPartitionCoord coord) throws Exception {
        MiruTenantTopology<?> topology = localTopologies.get(coord.tenantId);
        if (topology == null) {
            LOG.warn("Attempted to prioritize for unknown tenant {} (temporary = {})",
                coord.tenantId, routingTopologies.getIfPresent(coord.tenantId) != null);
            return false;
        }
        return prioritizeRebuildInternal(coord, topology);
    }

    private <BM> boolean prioritizeRebuildInternal(MiruPartitionCoord coord, MiruTenantTopology<BM> topology) throws Exception {
        Optional<MiruLocalHostedPartition<BM>> optionalPartition = topology.getPartition(coord.partitionId);
        if (optionalPartition.isPresent()) {
            MiruLocalHostedPartition<?> partition = optionalPartition.get();
            if (partition.getState() == MiruPartitionState.bootstrap) {
                tenantTopologyFactory.prioritizeRebuild(partition);
                return true;
            } else if (partition.getState() == MiruPartitionState.offline) {
                topology.warm(coord.partitionId);
                return true;
            } else if (partition.getState() == MiruPartitionState.online) {
                return topology.updateStorage(coord.partitionId, MiruBackingStorage.memory);
            }
        }
        return false;
    }

}
