package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.HostHeartbeat;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatRequest;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatResponse;
import com.jivesoftware.os.miru.api.topology.MiruIngressUpdate;
import com.jivesoftware.os.miru.api.topology.MiruPartitionStatus;
import com.jivesoftware.os.miru.api.topology.MiruTenantConfig;
import com.jivesoftware.os.miru.api.topology.MiruTopologyResponse;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class NoOpClusterClient implements MiruClusterClient {
    @Override
    public List<HostHeartbeat> allhosts() throws Exception {
        return Collections.emptyList();
    }

    @Override
    public MiruSchema getSchema(MiruTenantId tenantId) throws Exception {
        return null;
    }

    @Override
    public List<MiruPartition> partitions(MiruTenantId tenantId) throws Exception {
        return Collections.emptyList();
    }

    @Override
    public List<PartitionRange> getIngressRanges(MiruTenantId tenantId) throws Exception {
        return Collections.emptyList();
    }

    @Override
    public PartitionRange getIngressRange(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return null;
    }

    @Override
    public void registerSchema(MiruTenantId tenantId, MiruSchema schema) throws Exception {

    }

    @Override
    public void removeHost(MiruHost host) throws Exception {

    }

    @Override
    public void removeTopology(MiruHost host, MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {

    }

    @Override
    public MiruTenantConfig tenantConfig(MiruTenantId tenantId) throws Exception {
        return null;
    }

    @Override
    public void updateIngress(MiruIngressUpdate ingressUpdate) throws Exception {

    }

    @Override
    public void removeIngress(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {

    }

    @Override
    public void updateLastTimestamp(MiruPartitionCoord coord, long lastTimestamp) throws Exception {

    }

    @Override
    public void destroyPartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {

    }

    @Override
    public List<MiruPartitionStatus> getPartitionStatus(MiruTenantId tenantId, MiruPartitionId largestPartitionId) throws Exception {
        return Collections.emptyList();
    }

    @Override
    public MiruHeartbeatResponse thumpthump(MiruHost host, MiruHeartbeatRequest heartbeatRequest) throws Exception {
        return null;
    }

    @Override
    public MiruTopologyResponse routingTopology(MiruTenantId tenantId) throws Exception {
        return null;
    }
}
