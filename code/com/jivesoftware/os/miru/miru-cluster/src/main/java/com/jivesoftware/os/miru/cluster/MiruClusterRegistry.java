package com.jivesoftware.os.miru.cluster;

import com.google.common.base.Optional;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruTopologyStatus;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

public interface MiruClusterRegistry {

    void sendHeartbeatForHost(MiruHost miruHost) throws Exception;

    LinkedHashSet<HostHeartbeat> getAllHosts() throws Exception;

    MiruTenantConfig getTenantConfig(MiruTenantId tenantId) throws Exception;

    int getNumberOfReplicas(MiruTenantId tenantId) throws Exception;

    List<MiruTenantId> getTenantsForHost(MiruHost miruHost) throws Exception;

    void addToReplicaRegistry(MiruTenantId tenantId, MiruPartitionId partitionId, long nextId, MiruHost host) throws Exception;

    void removeTenantPartionReplicaSet(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    void ensurePartitionCoord(MiruPartitionCoord coord) throws Exception;

    Optional<MiruPartitionId> getLatestPartitionIdForTenant(MiruTenantId tenantId) throws Exception;

    List<MiruPartition> getPartitionsForTenant(MiruTenantId tenantId) throws Exception;

    List<MiruPartition> getPartitionsForTenantHost(MiruTenantId tenantId, MiruHost host) throws Exception;

    List<MiruTopologyStatus> getTopologyStatusForTenant(MiruTenantId tenantId) throws Exception;

    List<MiruTopologyStatus> getTopologyStatusForTenantHost(MiruTenantId tenantId, MiruHost host) throws Exception;

    Map<MiruPartitionId, MiruReplicaSet> getReplicaSets(MiruTenantId tenantId, Collection<MiruPartitionId> requiredPartitionId) throws Exception;

    void updateTopology(MiruPartitionCoord coord, Optional<MiruPartitionCoordInfo> optionalInfo,
        Optional<Long> refreshTimestamp) throws Exception;

    boolean isPartitionActive(MiruPartitionCoord coord) throws Exception;

    MiruPartition getPartition(MiruPartitionCoord coord) throws Exception;

    void removeHost(MiruHost host) throws Exception;

    void removeTopology(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) throws Exception;

    List<MiruTenantId> allTenantIds() throws Exception;

    void topologiesForTenants(List<MiruTenantId> tenantIds, final CallbackStream<MiruTopologyStatus> callbackStream) throws Exception;

    class HostHeartbeat {

        public final MiruHost host;
        public final long heartbeat;


        public HostHeartbeat(MiruHost host, long heartbeat) {
            this.host = host;
            this.heartbeat = heartbeat;
        }

        // only host contributes to equals()
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            HostHeartbeat that = (HostHeartbeat) o;

            if (host != null ? !host.equals(that.host) : that.host != null) {
                return false;
            }

            return true;
        }

        // only host contributes to hashCode()
        @Override
        public int hashCode() {
            return host != null ? host.hashCode() : 0;
        }
    }
}
