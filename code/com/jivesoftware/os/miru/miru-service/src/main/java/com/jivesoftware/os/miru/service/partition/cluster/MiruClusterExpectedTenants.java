package com.jivesoftware.os.miru.service.partition.cluster;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatResponse;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatResponse.Partition;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class MiruClusterExpectedTenants implements MiruExpectedTenants {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruClusterClient clusterClient;
    private final MiruTenantTopologyFactory tenantTopologyFactory;
    private final MiruRemoteQueryablePartitionFactory remotePartitionFactory;

    private final ConcurrentMap<MiruTenantId, MiruTenantTopology<?>> localTopologies = Maps.newConcurrentMap();
    private final StripingLocksProvider<MiruTenantId> tenantLocks = new StripingLocksProvider<>(64);

    private final Cache<MiruTenantId, MiruTenantRoutingTopology> routingTopologies = CacheBuilder.newBuilder()
        .concurrencyLevel(24)
        .expireAfterAccess(1, TimeUnit.HOURS)
        .maximumSize(10_000) //TODO configure
        .build();

    private final MiruPartitionHeartbeatHandler heartbeatHandler;
    private final MiruHostedPartitionComparison partitionComparison;

    public MiruClusterExpectedTenants(MiruTenantTopologyFactory tenantTopologyFactory,
        MiruRemoteQueryablePartitionFactory remotePartitionFactory,
        MiruPartitionHeartbeatHandler heartbeatHandler,
        MiruHostedPartitionComparison partitionComparison,
        MiruClusterClient clusterClient) {

        this.tenantTopologyFactory = tenantTopologyFactory;
        this.remotePartitionFactory = remotePartitionFactory;
        this.heartbeatHandler = heartbeatHandler;
        this.partitionComparison = partitionComparison;
        this.clusterClient = clusterClient;
    }

    @Override
    public Iterable<? extends OrderedPartitions<?>> allQueryablePartitionsInOrder(final MiruHost localhost,
        final MiruTenantId tenantId,
        String queryKey) throws Exception {

        MiruTenantRoutingTopology topology = routingTopologies.get(tenantId, new Callable<MiruTenantRoutingTopology>() {

            @Override
            public MiruTenantRoutingTopology call() throws Exception {

                MiruTopologyResponse topologyResponse = clusterClient.routingTopology(tenantId);
                ConcurrentSkipListMap<PartitionAndHost, MiruRoutablePartition> topology = new ConcurrentSkipListMap<PartitionAndHost, MiruRoutablePartition>();
                for (MiruTopologyResponse.Partition partition : topologyResponse.topology) {
                    MiruPartitionId partitionId = MiruPartitionId.of(partition.partitionId);
                    MiruRoutablePartition routablePartition = new MiruRoutablePartition(partition.host,
                        partitionId, partition.host.equals(localhost),
                        partition.state, partition.storage);
                    topology.put(new PartitionAndHost(partitionId, partition.host), routablePartition);
                }
                return new MiruTenantRoutingTopology(partitionComparison, topology);
            }
        });
        if (topology == null) {
            return Collections.emptyList();
        }
        final MiruTenantTopology<?> localTopology = localTopologies.get(tenantId);
        List<PartitionGroup> allPartitionsInOrder = topology.allPartitionsInOrder(tenantId, queryKey);
        return Lists.transform(allPartitionsInOrder, new Function<PartitionGroup, OrderedPartitions<?>>() {

            @Override
            public OrderedPartitions<?> apply(PartitionGroup input) {
                List<MiruQueryablePartition<?>> partitions = Lists.transform(input.partitions,
                    new Function<MiruRoutablePartition, MiruQueryablePartition<?>>() {

                        @Override
                        public MiruQueryablePartition<?> apply(MiruRoutablePartition input) {
                            MiruPartitionCoord key = new MiruPartitionCoord(tenantId, input.partitionId, input.host);
                            if (input.local) {
                                Optional<MiruLocalHostedPartition<?>> partition = localTopology.getPartition(key);
                                return partition.orNull();
                            } else {
                                return remotePartitionFactory.create(key, new MiruPartitionCoordInfo(input.state, input.storage));
                            }
                        }
                    });
                return new OrderedPartitions(input.tenantId, input.partitionId, Iterables.filter(partitions, Predicates.notNull()));
            }
        });
    }

    private final AtomicReference<Map<MiruTenantId, MiruTenantTopologyUpdate>> lastTopologyChanges =
        new AtomicReference<Map<MiruTenantId, MiruTenantTopologyUpdate>>(Maps.<MiruTenantId, MiruTenantTopologyUpdate>newHashMap());

    @Override
    public void thumpthump(MiruHost host) throws Exception {
        MiruHeartbeatResponse thumpthump = heartbeatHandler.thumpthump(host);

        ListMultimap<MiruTenantId, MiruPartitionId> tenantPartitionIds = ArrayListMultimap.create();
        for (Partition active : thumpthump.active) {
            tenantPartitionIds.put(active.tenantId, MiruPartitionId.of(active.partitionId));
        }
        expect(host, tenantPartitionIds);

        Map<MiruTenantId, MiruTenantTopologyUpdate> last = lastTopologyChanges.get();
        for (MiruTenantTopologyUpdate update : thumpthump.topologyHasChanged) {
            MiruTenantTopologyUpdate lastUpdate = last.get(update.tenantId);
            if (lastUpdate == null || lastUpdate.timestamp < update.timestamp) {
                routingTopologies.invalidate(update.tenantId);
            }
        }
        lastTopologyChanges.set(Maps.uniqueIndex(thumpthump.topologyHasChanged, new Function<MiruTenantTopologyUpdate, MiruTenantId>() {
            @Override
            public MiruTenantId apply(MiruTenantTopologyUpdate input) {
                return input.tenantId;
            }
        }));
    }

    private void expect(MiruHost host, ListMultimap<MiruTenantId, MiruPartitionId> expected) throws Exception {

        Set<MiruTenantId> synced = Sets.newHashSet();
        for (MiruTenantId tenantId : expected.keys()) {
            synchronized (tenantLocks.lock(tenantId)) {
                if (synced.add(tenantId)) {
                    MiruTenantTopology<?> tenantTopology = localTopologies.get(tenantId);
                    if (tenantTopology == null) {
                        tenantTopology = tenantTopologyFactory.create(tenantId);
                        localTopologies.put(tenantId, tenantTopology);
                    }

                    tenantTopology.checkForPartitionAlignment(host, tenantId, expected.get(tenantId));

                }
            }
        }

        for (MiruTenantId tenantId : localTopologies.keySet()) {
            synchronized (tenantLocks.lock(tenantId)) {
                if (!synced.contains(tenantId)) {
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
        Optional<MiruLocalHostedPartition<?>> optionalPartition = topology.getPartition(coord);
        if (optionalPartition.isPresent()) {
            MiruLocalHostedPartition<?> partition = optionalPartition.get();
            if (partition.getState() == MiruPartitionState.bootstrap) {
                tenantTopologyFactory.prioritizeRebuild(partition);
                return true;
            } else if (partition.getState() == MiruPartitionState.offline) {
                topology.warm(coord);
                return true;
            }
        }
        return false;
    }

}
