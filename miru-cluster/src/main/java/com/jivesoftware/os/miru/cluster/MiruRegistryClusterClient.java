package com.jivesoftware.os.miru.cluster;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruTopologyStatus;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.HostHeartbeat;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatRequest;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatResponse;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActiveUpdate;
import com.jivesoftware.os.miru.api.topology.MiruReplicaHosts;
import com.jivesoftware.os.miru.api.topology.MiruTenantConfig;
import com.jivesoftware.os.miru.api.topology.MiruTenantTopologyUpdate;
import com.jivesoftware.os.miru.api.topology.MiruTopologyPartition;
import com.jivesoftware.os.miru.api.topology.MiruTopologyResponse;
import com.jivesoftware.os.miru.api.topology.NamedCursorsResult;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author jonathan.colt
 */
public class MiruRegistryClusterClient implements MiruClusterClient {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruClusterRegistry clusterRegistry;

    public MiruRegistryClusterClient(MiruClusterRegistry clusterRegistry) {
        this.clusterRegistry = clusterRegistry;
    }

    @Override
    public List<HostHeartbeat> allhosts() throws Exception {
        return new ArrayList<>(clusterRegistry.getAllHosts());
    }

    @Override
    public void elect(MiruHost host, MiruTenantId tenantId, MiruPartitionId partitionId, long electionId) throws Exception {
        clusterRegistry.ensurePartitionCoord(new MiruPartitionCoord(tenantId, partitionId, host));
        clusterRegistry.addToReplicaRegistry(tenantId, partitionId, electionId, host);
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
    public void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {
        clusterRegistry.registerSchema(tenantId, schema);
    }

    @Override
    public void remove(MiruHost host) throws Exception {
        clusterRegistry.removeHost(host);
    }

    @Override
    public void remove(MiruHost host, MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        clusterRegistry.removeTopology(tenantId, partitionId, host);
    }

    @Override
    public void removeReplica(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        clusterRegistry.removeTenantPartionReplicaSet(tenantId, partitionId);
    }

    @Override
    public MiruReplicaHosts replicas(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        Map<MiruPartitionId, MiruReplicaSet> replicaSets = clusterRegistry.getReplicaSets(tenantId,
            Collections.singletonList(partitionId));
        MiruReplicaSet replicaSet = replicaSets.get(partitionId);

        return new MiruReplicaHosts(!replicaSet.get(MiruPartitionState.online).isEmpty(),
            replicaSet.getHostsWithReplica(),
            replicaSet.getCountOfMissingReplicas());
    }

    @Override
    public MiruTenantConfig tenantConfig(MiruTenantId tenantId) throws Exception {
        return clusterRegistry.getTenantConfig(tenantId);
    }

    @Override
    public MiruHeartbeatResponse thumpthump(final MiruHost miruHost, MiruHeartbeatRequest heartbeatRequest) throws Exception {
        clusterRegistry.sendHeartbeatForHost(miruHost);
        clusterRegistry.updateTopologies(miruHost, heartbeatRequest.active.stream()
            .map(partitionInfo -> {
                Optional<MiruPartitionCoordInfo> info = Optional.fromNullable(partitionInfo.info);
                Optional<Long> ingressTimestamp = (partitionInfo.ingressTimestamp > -1) ?
                    Optional.of(partitionInfo.ingressTimestamp) : Optional.<Long>absent();
                Optional<Long> queryTimestamp = (partitionInfo.queryTimestamp > -1) ?
                    Optional.of(partitionInfo.queryTimestamp) : Optional.<Long>absent();
                return new MiruClusterRegistry.TopologyUpdate(
                    new MiruPartitionCoord(partitionInfo.tenantId, MiruPartitionId.of(partitionInfo.partitionId), miruHost),
                    info,
                    ingressTimestamp,
                    queryTimestamp);
            })
            .collect(Collectors.toList()));

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
