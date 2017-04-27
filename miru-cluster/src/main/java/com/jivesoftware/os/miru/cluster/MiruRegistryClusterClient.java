package com.jivesoftware.os.miru.cluster;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.HostHeartbeat;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatRequest;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatResponse;
import com.jivesoftware.os.miru.api.topology.MiruIngressUpdate;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActiveUpdate;
import com.jivesoftware.os.miru.api.topology.MiruPartitionStatus;
import com.jivesoftware.os.miru.api.topology.MiruTenantConfig;
import com.jivesoftware.os.miru.api.topology.MiruTenantTopologyUpdate;
import com.jivesoftware.os.miru.api.topology.MiruTopologyPartition;
import com.jivesoftware.os.miru.api.topology.MiruTopologyResponse;
import com.jivesoftware.os.miru.api.topology.MiruTopologyStatus;
import com.jivesoftware.os.miru.api.topology.NamedCursorsResult;
import com.jivesoftware.os.miru.api.topology.PartitionInfo;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author jonathan.colt
 */
public class MiruRegistryClusterClient implements MiruClusterClient {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruClusterRegistry clusterRegistry;
    private final MiruReplicaSetDirector replicaSetDirector;
    private final Cache<TenantAndPartition, Boolean> replicationCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();

    public MiruRegistryClusterClient(MiruClusterRegistry clusterRegistry, MiruReplicaSetDirector replicaSetDirector) {
        this.clusterRegistry = clusterRegistry;
        this.replicaSetDirector = replicaSetDirector;
    }

    @Override
    public List<HostHeartbeat> allhosts() throws Exception {
        return new ArrayList<>(clusterRegistry.getAllHosts());
    }

    @Override
    public MiruSchema getSchema(MiruTenantId tenantId) throws Exception {
        return clusterRegistry.getSchema(tenantId);
    }

    @Override
    public List<MiruPartition> partitions(MiruTenantId tenantId) throws Exception {
        return clusterRegistry.getPartitionsForTenant(tenantId);
    }

    @Override
    public List<PartitionRange> getIngressRanges(MiruTenantId tenantId) throws Exception {
        return clusterRegistry.getIngressRanges(tenantId);
    }

    @Override
    public PartitionRange getIngressRange(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return clusterRegistry.getIngressRange(tenantId, partitionId);
    }

    @Override
    public void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        clusterRegistry.registerSchema(tenantId, schema);
    }

    @Override
    public void removeHost(MiruHost host) throws Exception {
        clusterRegistry.removeHost(host);
    }

    @Override
    public void removeTopology(MiruHost host, MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        clusterRegistry.removeTopology(tenantId, partitionId, host);
    }

    @Override
    public MiruTenantConfig tenantConfig(MiruTenantId tenantId) throws Exception {
        return clusterRegistry.getTenantConfig(tenantId);
    }

    @Override
    public void updateIngress(MiruIngressUpdate ingressUpdate) throws Exception {
        clusterRegistry.updateIngress(ingressUpdate);

        TenantAndPartition tenantAndPartition = new TenantAndPartition(ingressUpdate.tenantId, ingressUpdate.partitionId);
        if (replicationCache.getIfPresent(tenantAndPartition) == null) {
            MiruTenantId tenantId = ingressUpdate.tenantId;
            MiruPartitionId partitionId = ingressUpdate.partitionId;
            MiruReplicaSet replicaSet = clusterRegistry.getReplicaSet(tenantId, partitionId);
            if (replicaSet.getCountOfMissingReplicas() > 0) {
                LOG.debug("Electing {} replicas for {} {}", replicaSet.getCountOfMissingReplicas(), tenantId, partitionId);
                replicaSetDirector.electHostsForTenantPartition(tenantId, partitionId, replicaSet);
                replicationCache.put(tenantAndPartition, true);
            }
        }
    }

    @Override
    public void destroyPartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        LOG.info("Marking partition for destruction: {} {}", tenantId, partitionId);
        clusterRegistry.destroyPartition(tenantId, partitionId);
    }

    @Override
    public void removeIngress(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        clusterRegistry.removeIngress(tenantId, partitionId);

        TenantAndPartition tenantAndPartition = new TenantAndPartition(tenantId, partitionId);
        replicationCache.invalidate(tenantAndPartition);
    }

    @Override
    public void updateLastTimestamp(MiruPartitionCoord coord, long lastTimestamp) throws Exception {
        clusterRegistry.updateLastTimestamp(coord, lastTimestamp);
    }

    @Override
    public List<MiruPartitionStatus> getPartitionStatus(MiruTenantId tenantId, MiruPartitionId largestPartitionId) throws Exception {
        return clusterRegistry.getPartitionStatusForTenant(tenantId, largestPartitionId);
    }

    @Override
    public MiruHeartbeatResponse thumpthump(final MiruHost miruHost, MiruHeartbeatRequest heartbeatRequest) throws Exception {
        clusterRegistry.heartbeat(miruHost);
        clusterRegistry.updateTopologies(miruHost, heartbeatRequest.active.stream()
            .map(partitionInfo -> {
                Optional<MiruPartitionCoordInfo> info = Optional.fromNullable(partitionInfo.info);
                Optional<Long> queryTimestamp = (partitionInfo.queryTimestamp > -1) ?
                    Optional.of(partitionInfo.queryTimestamp) : Optional.<Long>absent();
                return new MiruClusterRegistry.TopologyUpdate(
                    new MiruPartitionCoord(partitionInfo.tenantId, MiruPartitionId.of(partitionInfo.partitionId), miruHost),
                    info,
                    queryTimestamp);
            })
            .collect(Collectors.toList()));
        for (PartitionInfo partitionInfo : heartbeatRequest.active) {
            if (partitionInfo.lastTimestamp != -1) {
                MiruPartitionCoord coord = new MiruPartitionCoord(partitionInfo.tenantId, MiruPartitionId.of(partitionInfo.partitionId), miruHost);
                updateLastTimestamp(coord, partitionInfo.lastTimestamp);
            }
        }

        NamedCursorsResult<Collection<MiruPartitionActiveUpdate>> partitionActiveHasChanged =
            clusterRegistry.getPartitionActiveUpdatesForHost(miruHost, heartbeatRequest.partitionActiveUpdatesSinceCursors);
        NamedCursorsResult<Collection<MiruTenantTopologyUpdate>> topologyHasChanged =
            clusterRegistry.getTopologyUpdatesForHost(miruHost, heartbeatRequest.topologyUpdatesSinceCursors);
        return new MiruHeartbeatResponse(partitionActiveHasChanged, topologyHasChanged);
    }

    @Override
    public MiruTopologyResponse routingTopology(MiruTenantId tenantId) throws Exception {
        List<MiruTopologyStatus> topologyStatusForTenant = clusterRegistry.getTopologyStatusForTenant(tenantId);
        ArrayList<MiruTopologyPartition> partitions = new ArrayList<>();
        for (MiruTopologyStatus status : topologyStatusForTenant) {
            if (status.destroyAfterTimestamp > 0 && System.currentTimeMillis() > status.destroyAfterTimestamp) {
                continue;
            }
            partitions.add(new MiruTopologyPartition(status.partition.coord.host,
                status.partition.coord.partitionId.getId(),
                status.partition.info.state,
                status.partition.info.storage,
                status.destroyAfterTimestamp));
        }
        return new MiruTopologyResponse(partitions);
    }

}
