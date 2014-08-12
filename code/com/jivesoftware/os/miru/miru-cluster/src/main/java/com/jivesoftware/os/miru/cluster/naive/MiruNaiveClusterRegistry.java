package com.jivesoftware.os.miru.cluster.naive;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

/** You get a tenant, and you get a tenant, and you get a tenant! Everybody gets a tenant! */
@Singleton
public class MiruNaiveClusterRegistry implements MiruClusterRegistry {

    private final Set<MiruHost> hostSet;

    @Inject
    public MiruNaiveClusterRegistry(@Named("miruDefaultHosts") List<MiruHost> miruHosts) {
        Preconditions.checkArgument(!miruHosts.isEmpty(), getClass().getSimpleName() + " requires default hosts");
        this.hostSet = Sets.newCopyOnWriteArraySet(miruHosts);
    }

    @Override
    public void sendHeartbeatForHost(MiruHost miruHost, long sizeInMemory, long sizeOnDisk) {
    }

    @Override
    public LinkedHashSet<HostHeartbeat> getAllHosts() {
        return Sets.newLinkedHashSet(Collections2.transform(hostSet, new Function<MiruHost, HostHeartbeat>() {
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
        return Collections.emptyList();
    }

    @Override
    public Set<MiruHost> electToReplicaSetForTenantPartition(MiruTenantId tenantId, MiruPartitionId partitionId, MiruReplicaSet replicaSet) {
        return Collections.emptySet();
    }

    @Override
    public MiruPartition getPartition(MiruPartitionCoord coord) throws Exception {
        return new MiruPartition(coord, new MiruPartitionCoordInfo(MiruPartitionState.online, MiruBackingStorage.memory));
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
        return new MiruReplicaSet(get(tenantId, Optional.of(partitionId), Optional.<MiruHost>absent()), ImmutableSet.copyOf(hostSet), 0);
    }

    @Override
    public void updateTopology(MiruPartitionCoord coord, MiruPartitionCoordInfo coordInfo, MiruPartitionCoordMetrics metrics, Optional<Long> refreshTimestamp)
        throws Exception {
    }

    @Override
    public void refreshTopology(MiruPartitionCoord coord, MiruPartitionCoordMetrics metrics, long refreshTimestamp) throws Exception {
    }

    @Override
    public boolean isPartitionActive(MiruPartitionCoord coord) throws Exception {
        return true;
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

    private ListMultimap<MiruPartitionState, MiruPartition> get(MiruTenantId tenantId, Optional<MiruPartitionId> partitionId,
        Optional<MiruHost> host) {
        return ArrayListMultimap.create(
            ImmutableMultimap.<MiruPartitionState, MiruPartition>builder()
                .putAll(MiruPartitionState.online, getPartitions(tenantId, partitionId, host))
                .build());
    }

    private Set<MiruPartition> getPartitions(MiruTenantId tenantId, Optional<MiruPartitionId> partitionId, Optional<MiruHost> requiredHost) {
        Set<MiruPartition> partitions = Sets.newHashSet();
        for (MiruHost host : hostSet) {
            if (!requiredHost.isPresent() || requiredHost.equals(host)) {
                MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId.or(MiruPartitionId.of(0)), host);
                MiruPartitionCoordInfo coordInfo = new MiruPartitionCoordInfo(MiruPartitionState.online, MiruBackingStorage.memory);
                partitions.add(new MiruPartition(coord, coordInfo));
            }
        }
        return partitions;
    }
}
