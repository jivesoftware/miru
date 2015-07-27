package com.jivesoftware.os.miru.api.topology;

import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;

/**
 * @author jonathan.colt
 */
public interface MiruClusterClient {

    List<HostHeartbeat> allhosts() throws Exception;

    MiruSchema getSchema(final MiruTenantId tenantId) throws Exception;

    List<MiruPartition> partitions(final MiruTenantId tenantId) throws Exception;

    void registerSchema(final MiruTenantId tenantId, final MiruSchema schema) throws Exception;

    boolean copySchema(MiruTenantId fromTenantId, List<MiruTenantId> toTenantIds) throws Exception;

    void removeHost(final MiruHost host) throws Exception;

    void removeTopology(final MiruHost host, final MiruTenantId tenantId, final MiruPartitionId partitionId) throws Exception;

    MiruTenantConfig tenantConfig(final MiruTenantId tenantId) throws Exception;

    void updateIngress(MiruIngressUpdate ingressUpdate) throws Exception;

    MiruHeartbeatResponse thumpthump(final MiruHost host, final MiruHeartbeatRequest heartbeatRequest) throws Exception;

    MiruTopologyResponse routingTopology(MiruTenantId tenantId) throws Exception;

}
