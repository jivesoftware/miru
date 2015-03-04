package com.jivesoftware.os.miru.cluster.client;

import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.HostHeartbeat;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatRequest;
import com.jivesoftware.os.miru.api.topology.MiruHeartbeatResponse;
import com.jivesoftware.os.miru.api.topology.MiruReplicaHosts;
import com.jivesoftware.os.miru.api.topology.MiruTenantConfig;
import com.jivesoftware.os.miru.api.topology.MiruTopologyResponse;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MiruHttpClusterClient implements MiruClusterClient {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final RequestHelper[] requestHelpers;
    private final AtomicInteger roundRobin = new AtomicInteger(0);

    public MiruHttpClusterClient(RequestHelper[] requestHelpers) {
        this.requestHelpers = requestHelpers;
    }

    private <R> R send(HttpCallable<R> send) {
        for (int i = 0; i < requestHelpers.length; i++) {
            int r = Math.abs(roundRobin.incrementAndGet()) % requestHelpers.length;
            try {
                return send.call(requestHelpers[r]);
            } catch (Exception x) {
                LOG.warn("Failed to send " + send, x);
            }
        }
        throw new RuntimeException("Failed to send.");
    }


    static interface HttpCallable<R> {

        R call(RequestHelper requestHelper);
    }

    @Override
    public MiruTopologyResponse routingTopology(final MiruTenantId tenantId) throws Exception {
        return send(new HttpCallable<MiruTopologyResponse>() {

            @Override
            public MiruTopologyResponse call(RequestHelper requestHelper) {
                return requestHelper.executeGetRequest("/miru/topology/routing/" + tenantId.toString(),
                    MiruTopologyResponse.class, null);
            }
        });
    }

    @Override
    public MiruHeartbeatResponse thumpthump(final MiruHost host, final MiruHeartbeatRequest heartbeatRequest) {
        return send(new HttpCallable<MiruHeartbeatResponse>() {

            @Override
            public MiruHeartbeatResponse call(RequestHelper requestHelper) {
                return requestHelper.executeRequest(heartbeatRequest,
                    "/miru/topology/thumpthump/" + host.getLogicalName() + "/" + host.getPort(),
                    MiruHeartbeatResponse.class, null);
            }
        });
    }

    @Override
    public List<HostHeartbeat> allhosts() {
        return send(new HttpCallable<List<HostHeartbeat>>() {
            @Override
            public List<HostHeartbeat> call(RequestHelper requestHelper) {
                return requestHelper.executeRequest(null, "/miru/topology/allHosts", List.class, new Class[]{HostHeartbeat.class}, null);
            }
        });
    }

    @Override
    public MiruTenantConfig tenantConfig(final MiruTenantId tenantId) {
        return send(new HttpCallable<MiruTenantConfig>() {
            @Override
            public MiruTenantConfig call(RequestHelper requestHelper) {
                return requestHelper.executeGetRequest("/miru/topology/tenantConfig/" + tenantId.toString(), MiruTenantConfig.class, null);
            }
        });
    }

    @Override
    public void elect(final MiruHost host, final MiruTenantId tenantId, final MiruPartitionId partitionId, final long electionId) {
        send(new HttpCallable<String>() {
            @Override
            public String call(RequestHelper requestHelper) {
                return requestHelper.executeRequest(null,
                    "/miru/topology/elect/"
                    + host.getLogicalName() + "/"
                    + host.getPort() + "/"
                    + tenantId + "/"
                    + partitionId.getId() + "/"
                    + electionId,
                    String.class, null);
            }
        });
    }

    @Override
    public void removeReplica(final MiruTenantId tenantId, final MiruPartitionId partitionId) {
        send(new HttpCallable<String>() {
            @Override
            public String call(RequestHelper requestHelper) {
                return requestHelper.executeRequest(null,
                    "/miru/topology/remove/replica/"
                    + tenantId + "/"
                    + partitionId.getId(),
                    String.class, null);
            }
        });
    }

    @Override
    public List<MiruPartition> partitions(final MiruTenantId tenantId) {
        return send(new HttpCallable<List<MiruPartition>>() {
            @Override
            public List<MiruPartition> call(RequestHelper requestHelper) {
                return requestHelper.executeRequest(null, "/miru/topology/partitions/" + tenantId.toString(),
                    List.class, new Class[]{MiruPartition.class}, null);
            }
        });
    }

    @Override
    public MiruReplicaHosts replicas(final MiruTenantId tenantId, final MiruPartitionId partitionId) {
        return send(new HttpCallable<MiruReplicaHosts>() {
            @Override
            public MiruReplicaHosts call(RequestHelper requestHelper) {
                return requestHelper.executeGetRequest("/miru/topology/replicas/" + tenantId.toString() + "/" + partitionId.getId(), MiruReplicaHosts.class,
                    null);
            }
        });
    }

    @Override
    public void remove(final MiruHost host) {
        send(new HttpCallable<String>() {
            @Override
            public String call(RequestHelper requestHelper) {
                return requestHelper.executeRequest(null,
                    "/miru/topology/remove/"
                    + host.getLogicalName() + "/"
                    + host.getPort(),
                    String.class, null);
            }
        });
    }

    @Override
    public void remove(final MiruHost host, final MiruTenantId tenantId, final MiruPartitionId partitionId) {
        send(new HttpCallable<String>() {
            @Override
            public String call(RequestHelper requestHelper) {
                return requestHelper.executeRequest(null,
                    "/miru/topology/remove/"
                    + host.getLogicalName() + "/"
                    + host.getPort() + "/"
                    + tenantId + "/"
                    + partitionId.getId(),
                    String.class, null);
            }
        });
    }

    @Override
    public MiruSchema getSchema(final MiruTenantId tenantId) {
        return send(new HttpCallable<MiruSchema>() {
            @Override
            public MiruSchema call(RequestHelper requestHelper) {
                return requestHelper.executeGetRequest("/miru/topology/schema/" + tenantId.toString(),
                    MiruSchema.class, null);
            }
        });
    }

    @Override
    public void registerSchema(final MiruTenantId tenantId, final MiruSchema schema) {
        send(new HttpCallable<String>() {
            @Override
            public String call(RequestHelper requestHelper) {
                return requestHelper.executeRequest(schema, "/miru/topology/schema/" + tenantId.toString(),
                    String.class, null);
            }
        });
    }
}
