package com.jivesoftware.os.miru.cluster;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
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
import com.jivesoftware.os.miru.api.topology.MiruPartitionActive;
import com.jivesoftware.os.miru.api.topology.MiruReplicaHosts;
import com.jivesoftware.os.miru.api.topology.MiruTenantConfig;
import com.jivesoftware.os.miru.api.topology.MiruTenantTopologyUpdate;
import com.jivesoftware.os.miru.api.topology.MiruTopologyResponse;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
        clusterRegistry.updateTopologies(miruHost, Lists.transform(heartbeatRequest.active,
            new Function<MiruHeartbeatRequest.Partition, MiruClusterRegistry.TopologyUpdate>() {
                @Override
                public MiruClusterRegistry.TopologyUpdate apply(MiruHeartbeatRequest.Partition partition) {
                    Optional<MiruPartitionCoordInfo> info = Optional.fromNullable(partition.info);
                    Optional<Long> refreshTimestamp = (partition.activeTimestamp > -1) ? Optional.of(partition.activeTimestamp) : Optional.<Long>absent();
                    return new MiruClusterRegistry.TopologyUpdate(
                        new MiruPartitionCoord(partition.tenantId, MiruPartitionId.of(partition.partitionId), miruHost),
                        info,
                        refreshTimestamp);
                }
            }));

        List<MiruHeartbeatResponse.Partition> partitions = new ArrayList<>();
        for (MiruTenantId tenantId : clusterRegistry.getTenantsForHost(miruHost)) {
            for (MiruTopologyStatus status : clusterRegistry.getTopologyStatusForTenantHost(tenantId, miruHost)) {
                MiruPartitionActive partitionActive = clusterRegistry.isPartitionActive(status.partition.coord);
                partitions.add(new MiruHeartbeatResponse.Partition(tenantId,
                    status.partition.coord.partitionId.getId(), partitionActive.active, partitionActive.idle));
            }
        }

        List<MiruTenantTopologyUpdate> topologyHasChangedForTheseTenantIds = clusterRegistry.getTopologyUpdatesForHost(miruHost,
            heartbeatRequest.topologyUpdatesSinceTimestamp);
        return new MiruHeartbeatResponse(partitions, topologyHasChangedForTheseTenantIds);
    }

    @Override
    public MiruTopologyResponse routingTopology(MiruTenantId tenantId) throws Exception {
        List<MiruTopologyStatus> topologyStatusForTenant = clusterRegistry.getTopologyStatusForTenant(tenantId);
        ArrayList<MiruTopologyResponse.Partition> partitions = new ArrayList<>();
        for (MiruTopologyStatus status : topologyStatusForTenant) {
            partitions.add(new MiruTopologyResponse.Partition(status.partition.coord.host,
                status.partition.coord.partitionId.getId(),
                status.partition.info.state,
                status.partition.info.storage));
        }
        return new MiruTopologyResponse(partitions);
    }

}
