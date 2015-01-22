package com.jivesoftware.os.miru.service.partition.cluster;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.plugin.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruExpectedTenants;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruPartitionInfoProvider;
import com.jivesoftware.os.miru.service.partition.MiruTenantTopology;
import com.jivesoftware.os.miru.service.partition.MiruTenantTopologyFactory;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

import static com.google.common.base.Preconditions.checkArgument;

/**
 *
 */
public class MiruClusterExpectedTenants implements MiruExpectedTenants {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruPartitionInfoProvider partitionInfoProvider;
    private final MiruClusterRegistry clusterRegistry;
    private final MiruTenantTopologyFactory tenantTopologyFactory;

    private final ConcurrentMap<MiruTenantId, MiruTenantTopology<?>> expectedTopologies = Maps.newConcurrentMap();
    private final StripingLocksProvider<MiruTenantId> tenantLocks = new StripingLocksProvider<>(64);

    private final Cache<MiruTenantId, MiruTenantTopology<?>> temporaryTopologies = CacheBuilder.newBuilder()
            .concurrencyLevel(24)
            .expireAfterAccess(1, TimeUnit.HOURS)
            .maximumSize(10_000) //TODO configure
            .removalListener(new RemovalListener<MiruTenantId, MiruTenantTopology<?>>() {
                @Override
                public void onRemoval(@Nonnull RemovalNotification<MiruTenantId, MiruTenantTopology<?>> notification) {
                    MiruTenantId tenantId = notification.getKey();
                    MiruTenantTopology<?> tenantTopology = notification.getValue();

                    if (tenantTopology != null && tenantId != null) {
                        if (expectedTopologies.get(tenantId) != tenantTopology) {
                            LOG.info("Removed temporary topology for {}: {}", tenantId, notification.getCause());
                            Iterator<MiruPartitionCoord> iter = partitionInfoProvider.getKeysIterator();
                            while (iter.hasNext()) {
                                MiruPartitionCoord coord = iter.next();
                                if (coord.tenantId.equals(tenantId)) {
                                    iter.remove();
                                }
                            }
                            tenantTopology.remove();
                        } else {
                            LOG.info("Migrated temporary topology for {}", tenantId);
                        }
                    }
                }
            })
            .build();
    private final ConcurrentLinkedDeque<MiruTenantTopology<?>> temporaryUpdateDeque = new ConcurrentLinkedDeque<>();

    public MiruClusterExpectedTenants(MiruPartitionInfoProvider partitionInfoProvider,
            MiruTenantTopologyFactory tenantTopologyFactory,
            MiruClusterRegistry clusterRegistry) {

        this.partitionInfoProvider = partitionInfoProvider;
        this.tenantTopologyFactory = tenantTopologyFactory;
        this.clusterRegistry = clusterRegistry;

    }

    @Override
    public MiruTenantTopology<?> getTopology(final MiruTenantId tenantId) throws Exception {
        MiruTenantTopology<?> tenantTopology = expectedTopologies.get(tenantId);
        if (tenantTopology == null) {
            synchronized (tenantLocks.lock(tenantId)) {
                tenantTopology = temporaryTopologies.get(tenantId, new Callable<MiruTenantTopology<?>>() {
                    @Override
                    public MiruTenantTopology<?> call() throws Exception {
                        LOG.info("Added temporary topology for {}", tenantId);
                        MiruTenantTopology<?> temporaryTopology = tenantTopologyFactory.create(tenantId);
                        alignTopology(temporaryTopology);
                        return temporaryTopology;
                    }
                });
                temporaryUpdateDeque.add(tenantTopology);
            }
        }
        return tenantTopology;
    }

    @Override
    public Collection<MiruTenantTopology<?>> topologies() {
        return expectedTopologies.values();
    }

    @Override
    public boolean isExpected(MiruTenantId tenantId) {
        return expectedTopologies.containsKey(tenantId);
    }

    @Override
    public void expect(List<MiruTenantId> expectedTenantsForHost) throws Exception {
        checkArgument(expectedTenantsForHost != null);

        Set<MiruTenantId> synced = Sets.newHashSet();
        for (MiruTenantId tenantId : expectedTenantsForHost) {
            synchronized (tenantLocks.lock(tenantId)) {
                if (synced.add(tenantId)) {
                    MiruTenantTopology<?> tenantTopology = expectedTopologies.get(tenantId);
                    if (tenantTopology == null) {
                        MiruTenantTopology<?> allowed = temporaryTopologies.getIfPresent(tenantId);
                        if (allowed != null) {
                            tenantTopology = allowed;
                        } else {
                            tenantTopology = tenantTopologyFactory.create(tenantId);
                        }
                        expectedTopologies.putIfAbsent(tenantId, tenantTopology);
                    }

                    alignTopology(tenantTopology);
                    temporaryTopologies.invalidate(tenantId);
                }
            }
        }

        for (MiruTenantId tenantId : expectedTopologies.keySet()) {
            synchronized (tenantLocks.lock(tenantId)) {
                if (!synced.contains(tenantId)) {
                    final MiruTenantTopology removed = expectedTopologies.remove(tenantId);
                    if (removed != null) {
                        // tenant is no longer expected, but instead of removing demote it to temporary, and enqueue for alignment
                        temporaryTopologies.put(tenantId, removed);
                        temporaryUpdateDeque.add(removed);
                    }
                }
            }
        }

        // stop after we reach the current oldest element
        MiruTenantTopology<?> lastTemporaryTopology = temporaryUpdateDeque.peekLast();
        while (!temporaryUpdateDeque.isEmpty()) {
            MiruTenantTopology<?> tenantTopology = temporaryUpdateDeque.removeFirst();
            MiruTenantId tenantId = tenantTopology.getTenantId();
            synchronized (tenantLocks.lock(tenantId)) {
                if (synced.add(tenantId)) {
                    alignTopology(tenantTopology);
                }
                if (tenantTopology == lastTemporaryTopology) {
                    break;
                }
            }
        }
    }

    @Override
    public boolean prioritizeRebuild(MiruPartitionCoord coord) {
        MiruTenantTopology<?> topology = expectedTopologies.get(coord.tenantId);
        Optional<MiruHostedPartition<?>> optionalPartition = topology.getPartition(coord);
        if (optionalPartition.isPresent()) {
            MiruHostedPartition<?> partition = optionalPartition.get();
            if (partition.isLocal()) {
                if (partition.getState() == MiruPartitionState.bootstrap) {
                    tenantTopologyFactory.prioritizeRebuild((MiruLocalHostedPartition<?>) partition);
                    return true;
                } else if (partition.getState() == MiruPartitionState.offline) {
                    topology.warm(coord);
                    return true;
                }
            }
        }
        return false;
    }

    private void alignTopology(MiruTenantTopology<?> tenantTopology) throws Exception {
        MiruTenantId tenantId = tenantTopology.getTenantId();

        List<MiruPartition> partitionsForTenant = clusterRegistry.getPartitionsForTenant(tenantId);
        for (MiruPartition partition : partitionsForTenant) {
            partitionInfoProvider.put(partition.coord, partition.info);
        }
        tenantTopology.checkForPartitionAlignment(Lists.transform(partitionsForTenant,
            new Function<MiruPartition, MiruPartitionCoord>() {
                @Override
                public MiruPartitionCoord apply(MiruPartition input) {
                    return input.coord;
                }
            }));
    }
}
