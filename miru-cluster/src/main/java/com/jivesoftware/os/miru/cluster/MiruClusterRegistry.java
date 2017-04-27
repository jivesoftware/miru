package com.jivesoftware.os.miru.cluster;

import com.google.common.base.Optional;
import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.HostHeartbeat;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient.PartitionRange;
import com.jivesoftware.os.miru.api.topology.MiruIngressUpdate;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActiveUpdate;
import com.jivesoftware.os.miru.api.topology.MiruPartitionStatus;
import com.jivesoftware.os.miru.api.topology.MiruTenantConfig;
import com.jivesoftware.os.miru.api.topology.MiruTenantTopologyUpdate;
import com.jivesoftware.os.miru.api.topology.MiruTopologyStatus;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import com.jivesoftware.os.miru.api.topology.NamedCursorsResult;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

public interface MiruClusterRegistry {

    void heartbeat(MiruHost miruHost) throws Exception;

    LinkedHashSet<HostHeartbeat> getAllHosts() throws Exception;

    MiruTenantConfig getTenantConfig(MiruTenantId tenantId) throws Exception;

    int getNumberOfReplicas(MiruTenantId tenantId) throws Exception;

    List<MiruTenantId> getTenantsForHost(MiruHost miruHost) throws Exception;

    void addToReplicaRegistry(ListMultimap<MiruHost, TenantAndPartition> coords, long nextId) throws Exception;

    void removeTenantPartionReplicaSet(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    void ensurePartitionCoords(ListMultimap<MiruHost, TenantAndPartition> coords) throws Exception;

    List<MiruPartition> getPartitionsForTenant(MiruTenantId tenantId) throws Exception;

    void debugTenant(MiruTenantId tenantId, StringBuilder stringBuilder) throws Exception;

    void debugTenantPartition(MiruTenantId tenantId, MiruPartitionId partitionId, StringBuilder builder) throws Exception;

    List<MiruPartition> getPartitionsForTenantHost(MiruTenantId tenantId, MiruHost host) throws Exception;

    List<MiruPartitionStatus> getPartitionStatusForTenant(MiruTenantId tenantId, MiruPartitionId largestPartitionId) throws Exception;

    List<MiruTopologyStatus> getTopologyStatusForTenant(MiruTenantId tenantId) throws Exception;

    List<MiruTopologyStatus> getTopologyStatusForTenantHost(MiruTenantId tenantId, MiruHost host) throws Exception;

    MiruReplicaSet getReplicaSet(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    Map<MiruPartitionId, MiruReplicaSet> getReplicaSets(MiruTenantId tenantId, Collection<MiruPartitionId> requiredPartitionId) throws Exception;

    void updateIngress(MiruIngressUpdate ingressUpdate) throws Exception;

    void removeIngress(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    void updateLastTimestamp(MiruPartitionCoord coord, long lastTimestamp) throws Exception;

    void destroyPartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    void updateTopologies(MiruHost host, Collection<TopologyUpdate> topologyUpdates) throws Exception;

    NamedCursorsResult<Collection<MiruTenantTopologyUpdate>> getTopologyUpdatesForHost(MiruHost host,
        Collection<NamedCursor> sinceCursors) throws Exception;

    NamedCursorsResult<Collection<MiruPartitionActiveUpdate>> getPartitionActiveUpdatesForHost(MiruHost host,
        Collection<NamedCursor> sinceCursors) throws Exception;

    void removeHost(MiruHost host) throws Exception;

    void removeTopology(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) throws Exception;

    void topologiesForTenants(List<MiruTenantId> tenantIds, final CallbackStream<MiruTopologyStatus> callbackStream) throws Exception;

    MiruSchema getSchema(MiruTenantId tenantId) throws Exception;

    void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception;

    boolean copySchema(MiruTenantId fromTenantId, List<MiruTenantId> toTenantIds) throws Exception;

    int upgradeSchema(MiruSchema schema, boolean upgradeOnMissing, boolean upgradeOnError) throws Exception;

    List<PartitionRange> getIngressRanges(MiruTenantId tenantId) throws Exception;

    PartitionRange getIngressRange(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    class TopologyUpdate {
        public final MiruPartitionCoord coord;
        public final Optional<MiruPartitionCoordInfo> optionalInfo;
        public final Optional<Long> queryTimestamp;

        public TopologyUpdate(MiruPartitionCoord coord,
            Optional<MiruPartitionCoordInfo> optionalInfo,
            Optional<Long> queryTimestamp) {
            this.coord = coord;
            this.optionalInfo = optionalInfo;
            this.queryTimestamp = queryTimestamp;
        }
    }
}
