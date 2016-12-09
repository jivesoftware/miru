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
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.CoordinateStream;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatResponse;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActiveUpdate;
import com.jivesoftware.os.miru.api.topology.MiruTenantTopologyUpdate;
import com.jivesoftware.os.miru.api.topology.MiruTopologyPartition;
import com.jivesoftware.os.miru.api.topology.MiruTopologyResponse;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.miru.plugin.partition.MiruRoutablePartition;
import com.jivesoftware.os.miru.plugin.partition.OrderedPartitions;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.plugin.solution.MiruTimeRange;
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

    private final ConcurrentMap<MiruTenantId, MiruTenantTopology<?, ?>> localTopologies = Maps.newConcurrentMap();
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
    public Iterable<? extends OrderedPartitions<?, ?>> allQueryablePartitionsInOrder(final MiruTenantId tenantId,
        String requestName,
        String queryKey) throws Exception {

        MiruTenantRoutingTopology topology = getTenantRoutingTopology(tenantId);
        if (topology == null) {
            return Collections.emptyList();
        }
        MiruTenantTopology<?, ?> localTopology = localTopologies.get(tenantId);
        return getOrderedPartitions(tenantId, requestName, queryKey, topology, (MiruTenantTopology) localTopology);
    }

    @Override
    public OrderedPartitions<?, ?> queryablePartitionInOrder(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        String requestName,
        String queryKey) throws Exception {

        MiruTenantRoutingTopology topology = getTenantRoutingTopology(tenantId);
        if (topology == null) {
            return null;
        }
        PartitionGroup partitionGroup = topology.partitionInOrder(tenantId, partitionId, requestName, queryKey);
        final MiruTenantTopology<?, ?> localTopology = localTopologies.get(tenantId);
        return getOrderedPartition(tenantId, (MiruTenantTopology) localTopology, partitionGroup);
    }

    private MiruTenantRoutingTopology getTenantRoutingTopology(MiruTenantId tenantId) throws Exception {
        return routingTopologies.get(tenantId, () -> {

            MiruTopologyResponse topologyResponse = clusterClient.routingTopology(tenantId);
            ConcurrentSkipListMap<PartitionAndHost, MiruRoutablePartition> partitionHostTopology = new ConcurrentSkipListMap<>();
            for (MiruTopologyPartition partition : topologyResponse.topology) {
                MiruPartitionId partitionId = MiruPartitionId.of(partition.partitionId);
                MiruRoutablePartition routablePartition = new MiruRoutablePartition(partition.host,
                    partitionId, partition.host.equals(localHost),
                    partition.state, partition.storage, partition.destroyAfterTimestamp);
                partitionHostTopology.put(new PartitionAndHost(partitionId, partition.host), routablePartition);
            }
            return new MiruTenantRoutingTopology(partitionComparison, partitionHostTopology);
        });
    }

    private <BM extends IBM, IBM> List<OrderedPartitions<BM, IBM>> getOrderedPartitions(MiruTenantId tenantId,
        String requestName,
        String queryKey,
        MiruTenantRoutingTopology topology,
        MiruTenantTopology<BM, IBM> localTopology) {

        List<PartitionGroup> allPartitionsInOrder = topology.allPartitionsInOrder(tenantId, requestName, queryKey);
        return Lists.transform(allPartitionsInOrder, input -> {
            return getOrderedPartition(tenantId, localTopology, input);
        });
    }

    private <BM extends IBM, IBM> OrderedPartitions<BM, IBM> getOrderedPartition(MiruTenantId tenantId,
        MiruTenantTopology<BM, IBM> localTopology,
        PartitionGroup partitionGroup) {
        List<MiruQueryablePartition<BM, IBM>> partitions = Lists.transform(partitionGroup.partitions,
            routablePartition -> {
                MiruPartitionCoord key = new MiruPartitionCoord(tenantId, routablePartition.partitionId, routablePartition.host);
                if (routablePartition.local) {
                    if (localTopology == null) {
                        return null;
                    } else {
                        Optional<MiruLocalHostedPartition<BM, IBM, ?, ?>> partition = localTopology.getPartition(routablePartition.partitionId);
                        return partition.orNull();
                    }
                } else {
                    return remotePartitionFactory.create(key);
                }
            });
        // uncomment to prevent local queries
        //return new OrderedPartitions<>(input.tenantId, input.partitionId, Iterables.filter(partitions, input1 -> input1 != null && !input1.isLocal()));
        return new OrderedPartitions<>(partitionGroup.tenantId, partitionGroup.partitionId, Iterables.filter(partitions, Predicates.notNull()));
    }

    @Override
    public void thumpthump() throws Exception {
        MiruHeartbeatResponse thumpthump = heartbeatHandler.thumpthump(localHost);
        ListMultimap<MiruTenantId, MiruPartitionActiveUpdate> tenantPartitionActive = ArrayListMultimap.create();
        if (thumpthump == null) {
            LOG.warn("Heartbeat was empty");
        } else {
            if (thumpthump.activeHasChanged == null || thumpthump.activeHasChanged.result == null) {
                LOG.warn("Thumpthumps activeHasChanged was empty");
            } else {
                for (MiruPartitionActiveUpdate active : thumpthump.activeHasChanged.result) {
                    tenantPartitionActive.put(active.tenantId, active);
                }
                expect(tenantPartitionActive);
            }

            if (thumpthump.topologyHasChanged == null || thumpthump.topologyHasChanged.result == null) {
                LOG.warn("Thumpthumps topologyHasChanged was empty");
            } else {
                for (MiruTenantTopologyUpdate update : thumpthump.topologyHasChanged.result) {
                    routingTopologies.invalidate(update.tenantId);
                }
            }
        }
    }

    private void expect(ListMultimap<MiruTenantId, MiruPartitionActiveUpdate> expected) throws Exception {

        for (MiruTenantId tenantId : expected.keys()) {
            synchronized (tenantLocks.lock(tenantId, 0)) {
                MiruTenantTopology<?, ?> tenantTopology = localTopologies.get(tenantId);
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
    public MiruTenantTopology<?, ?> getLocalTopology(final MiruTenantId tenantId) throws Exception {
        return localTopologies.get(tenantId);
    }

    @Override
    public boolean prioritizeRebuild(MiruPartitionCoord coord) throws Exception {
        MiruTenantTopology<?, ?> topology = localTopologies.get(coord.tenantId);
        if (topology == null) {
            LOG.warn("Attempted to prioritize for unknown tenant {} (temporary = {})",
                coord.tenantId, routingTopologies.getIfPresent(coord.tenantId) != null);
            return false;
        }
        return prioritizeRebuildInternal(coord, (MiruTenantTopology) topology);
    }

    private <BM extends IBM, IBM> boolean prioritizeRebuildInternal(MiruPartitionCoord coord,
        MiruTenantTopology<BM, IBM> topology) throws Exception {

        Optional<MiruLocalHostedPartition<BM, IBM, ?, ?>> optionalPartition = topology.getPartition(coord.partitionId);
        if (optionalPartition.isPresent()) {
            MiruLocalHostedPartition<BM, IBM, ?, ?> partition = optionalPartition.get();
            if (partition.getState() == MiruPartitionState.bootstrap || partition.getState() == MiruPartitionState.obsolete) {
                tenantTopologyFactory.prioritizeRebuild(partition);
                return true;
            } else if (partition.getState() == MiruPartitionState.offline) {
                topology.warm(coord.partitionId);
                return true;
            } else if (partition.getState() == MiruPartitionState.online) {
                return topology.rebuild(coord.partitionId);
            }
        }
        return false;
    }

    @Override
    public boolean compact(MiruPartitionCoord coord) throws Exception {
        MiruTenantTopology<?, ?> topology = localTopologies.get(coord.tenantId);
        if (topology == null) {
            LOG.warn("Attempted to compact for unknown tenant {} (temporary = {})",
                coord.tenantId, routingTopologies.getIfPresent(coord.tenantId) != null);
            return false;
        }
        topology.compact(coord.partitionId);
        return true;
    }

    @Override
    public boolean rebuildTimeRange(MiruTimeRange miruTimeRange, boolean hotDeploy, boolean chunkStores, boolean labIndex) throws Exception {
        for (Map.Entry<MiruTenantId, MiruTenantTopology<?, ?>> entry : localTopologies.entrySet()) {
            MiruTenantTopology<?, ?> topology = entry.getValue();
            for (MiruLocalHostedPartition<?, ?, ?, ?> hostedPartition : entry.getValue().allPartitions()) {
                MiruPartitionCoord coord = hostedPartition.getCoord();
                try (MiruRequestHandle<?, ?, ?> handle = hostedPartition.inspectRequestHandle(hotDeploy)) {
                    MiruRequestContext<?, ?, ? extends MiruSipCursor<?>> requestContext = handle.getRequestContext();
                    MiruTimeIndex timeIndex = requestContext.getTimeIndex();
                    boolean hasChunkStores = requestContext.hasChunkStores();
                    boolean hasLabIndex = requestContext.hasLabIndex();
                    if (timeIndex.intersects(miruTimeRange) && (chunkStores && hasChunkStores || labIndex && hasLabIndex)) {
                        LOG.info("Rebuild requested for {} with intersecting time range, hasChunkStores={} hasLabIndex={}",
                            coord, hasChunkStores, hasLabIndex);
                        topology.rebuild(coord.partitionId);
                    }
                } catch (Exception x) {
                    LOG.warn("Attempt to rebuild offline non disk partition was ignored for {}.", new Object[] { coord }, x);
                }
            }
        }
        return true;
    }

    @Override
    public boolean expectedTopologies(Optional<MiruTenantId> tenantId, CoordinateStream stream) throws Exception {
        if (tenantId.isPresent()) {
            MiruTenantTopology<?, ?> tenantTopology = localTopologies.get(tenantId.get());
            if (tenantTopology != null) {
                for (MiruLocalHostedPartition<?, ?, ?, ?> hostedPartition : tenantTopology.allPartitions()) {
                    MiruPartitionCoord coord = hostedPartition.getCoord();
                    if (!stream.stream(coord.tenantId, coord.partitionId, coord.host)) {
                        return false;
                    }
                }
            }
        } else {
            for (Map.Entry<MiruTenantId, MiruTenantTopology<?, ?>> entry : localTopologies.entrySet()) {
                for (MiruLocalHostedPartition<?, ?, ?, ?> hostedPartition : entry.getValue().allPartitions()) {
                    MiruPartitionCoord coord = hostedPartition.getCoord();
                    if (!stream.stream(coord.tenantId, coord.partitionId, coord.host)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

}
