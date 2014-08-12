package com.jivesoftware.os.miru.cluster.memory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionCoordMetrics;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruTopologyStatus;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruReplicaSet;
import com.jivesoftware.os.miru.cluster.MiruTenantConfig;
import com.jivesoftware.os.miru.cluster.MiruTenantConfigFields;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.inject.Singleton;

/** @author jonathan */
@Singleton
public class MiruInMemoryClusterRegistry implements MiruClusterRegistry {

    private final Multimap<MiruTenantId, MiruPartitionCoord> tenantIdToCoords;
    private final Multimap<MiruTenantId, MiruHost> tenantIdToHosts;
    private final Multimap<MiruHost, MiruTenantId> hostToTenantIds;
    private final Map<MiruPartitionCoord, Long> coordRefreshTimestamp;

    public MiruInMemoryClusterRegistry() {
        this.tenantIdToCoords = Multimaps.synchronizedSetMultimap(HashMultimap.<MiruTenantId, MiruPartitionCoord>create());
        this.tenantIdToHosts = Multimaps.synchronizedSetMultimap(HashMultimap.<MiruTenantId, MiruHost>create());
        this.hostToTenantIds = Multimaps.synchronizedSetMultimap(HashMultimap.<MiruHost, MiruTenantId>create());
        this.coordRefreshTimestamp = Maps.newConcurrentMap();
    }

    public void addTenantHosts(MiruTenantId tenantId, List<MiruHost> hosts) {
        this.tenantIdToHosts.putAll(tenantId, hosts);
        for (MiruHost host : hosts) {
            this.hostToTenantIds.put(host, tenantId);
        }
    }

    public void removeTenantHosts(MiruTenantId tenantId, List<MiruHost> hosts) {
        for (MiruHost host : hosts) {
            this.tenantIdToHosts.remove(tenantId, host);
            this.hostToTenantIds.removeAll(host);
        }
    }

    public void addTenantCoords(MiruTenantId tenantId, List<MiruPartitionCoord> coords) {
        this.tenantIdToCoords.putAll(tenantId, coords);
    }

    public void removeTenantCoords(MiruTenantId tenantId, List<MiruPartitionCoord> coords) {
        for (MiruPartitionCoord coord : coords) {
            this.tenantIdToCoords.remove(tenantId, coord);
        }
    }

    @Override
    public void sendHeartbeatForHost(MiruHost miruHost, long sizeInMemory, long sizeOnDisk) {
    }

    @Override
    public LinkedHashSet<HostHeartbeat> getAllHosts() {
        return Sets.newLinkedHashSet(Collections2.transform(hostToTenantIds.keySet(), new Function<MiruHost, HostHeartbeat>() {
            @Nullable
            @Override
            public HostHeartbeat apply(@Nullable MiruHost input) {
                return new HostHeartbeat(input, 0, -1, -1);
            }
        }));
    }

    @Override
    public MiruTenantConfig getTenantConfig(MiruTenantId tenantId) throws Exception {
        return new MiruTenantConfig(Collections.<MiruTenantConfigFields, Long>emptyMap());
    }

    @Override
    public List<MiruTenantId> getTenantsForHost(MiruHost miruHost) {
        return Lists.newArrayList(hostToTenantIds.get(miruHost));
    }

    @Override
    public Set<MiruHost> electToReplicaSetForTenantPartition(MiruTenantId tenantId, MiruPartitionId partitionId, MiruReplicaSet replicaSet) {
        return Collections.emptySet();
    }

    @Override
    public ListMultimap<MiruPartitionState, MiruPartition> getPartitionsForTenant(MiruTenantId tenantId) {
        return get(tenantId, Optional.<MiruPartitionId>absent(), Optional.<MiruHost>absent());
    }

    @Override
    public ListMultimap<MiruPartitionState, MiruPartition> getPartitionsForTenantHost(MiruTenantId tenantId, MiruHost host) {
        return get(tenantId, Optional.<MiruPartitionId>absent(), Optional.of(host));
    }

    @Override
    public ListMultimap<MiruPartitionState, MiruTopologyStatus> getTopologyStatusForTenant(MiruTenantId tenantId) throws Exception {
        return Multimaps.transformValues(
            get(tenantId, Optional.<MiruPartitionId>absent(), Optional.<MiruHost>absent()),
            new Function<MiruPartition, MiruTopologyStatus>() {
                @Nullable
                @Override
                public MiruTopologyStatus apply(@Nullable MiruPartition input) {
                    return new MiruTopologyStatus(input, new MiruPartitionCoordMetrics(-1, -1));
                }
            });
    }

    @Override
    public ListMultimap<MiruPartitionState, MiruTopologyStatus> getTopologyStatusForTenantHost(MiruTenantId tenantId, MiruHost host) throws Exception {
        return Multimaps.transformValues(
            get(tenantId, Optional.<MiruPartitionId>absent(), Optional.of(host)),
            new Function<MiruPartition, MiruTopologyStatus>() {
                @Nullable
                @Override
                public MiruTopologyStatus apply(@Nullable MiruPartition input) {
                    return new MiruTopologyStatus(input, new MiruPartitionCoordMetrics(-1, -1));
                }
            });
    }

    @Override
    public MiruReplicaSet getReplicaSet(MiruTenantId tenantId, MiruPartitionId partitionId) {
        ListMultimap<MiruPartitionState, MiruPartition> partitionsByState = get(tenantId, Optional.of(partitionId), Optional.<MiruHost>absent());
        HashSet<MiruHost> hostsWithReplica = Sets.newHashSet(Collections2.transform(partitionsByState.values(), partitionToHost));
        return new MiruReplicaSet(partitionsByState, hostsWithReplica, 0);
    }

    @Override
    public void updateTopology(MiruPartitionCoord coord, MiruPartitionCoordInfo coordInfo, MiruPartitionCoordMetrics metrics, Optional<Long> refreshTimestamp)
        throws Exception {

        if (refreshTimestamp.isPresent()) {
            coordRefreshTimestamp.put(coord, refreshTimestamp.get());
        }
    }

    @Override
    public void refreshTopology(MiruPartitionCoord coord, MiruPartitionCoordMetrics metrics, long refreshTimestamp) throws Exception {
        coordRefreshTimestamp.put(coord, refreshTimestamp);
    }

    @Override
    public boolean isPartitionActive(MiruPartitionCoord coord) throws Exception {
        Long refreshTimestamp = coordRefreshTimestamp.get(coord);
        return refreshTimestamp != null && refreshTimestamp > System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
    }

    @Override
    public MiruPartition getPartition(MiruPartitionCoord coord) throws Exception {
        return new MiruPartition(coord, new MiruPartitionCoordInfo(MiruPartitionState.online, MiruBackingStorage.memory));
    }

    @Override
    public void removeHost(MiruHost host) throws Exception {
    }

    @Override
    public void removeReplicas(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
    }

    @Override
    public void moveReplica(MiruTenantId tenantId, MiruPartitionId partitionId, Optional<MiruHost> fromHost, MiruHost toHost) throws Exception {
    }

    @Override
    public void removeTopology(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) throws Exception {
    }

    private final Function<MiruPartition, MiruHost> partitionToHost = new Function<MiruPartition, MiruHost>() {
        @Nullable
        @Override
        public MiruHost apply(@Nullable MiruPartition input) {
            return input != null ? input.coord.host : null;
        }
    };

    private ListMultimap<MiruPartitionState, MiruPartition> get(MiruTenantId tenantId, Optional<MiruPartitionId> requiredPartitionId,
        Optional<MiruHost> requiredHost) {
        return ArrayListMultimap.create(
            ImmutableMultimap.<MiruPartitionState, MiruPartition>builder()
                .putAll(MiruPartitionState.online, getPartitions(tenantId, requiredPartitionId, requiredHost))
                .build());
    }

    private Set<MiruPartition> getPartitions(MiruTenantId tenantId, Optional<MiruPartitionId> requiredPartitionId, Optional<MiruHost> requiredHost) {
        Set<MiruPartition> partitions = Sets.newHashSet();
        for (MiruPartitionCoord coord : tenantIdToCoords.get(tenantId)) {
            if ((!requiredPartitionId.isPresent() || coord.partitionId.equals(requiredPartitionId.get())) &&
                (!requiredHost.isPresent() || coord.host.equals(requiredHost.get()))) {
                MiruPartitionCoordInfo coordInfo = new MiruPartitionCoordInfo(MiruPartitionState.online, MiruBackingStorage.memory);
                partitions.add(new MiruPartition(coord, coordInfo));
            }
        }
        return partitions;
    }
}
