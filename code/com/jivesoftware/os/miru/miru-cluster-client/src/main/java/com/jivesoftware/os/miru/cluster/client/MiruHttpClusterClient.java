package com.jivesoftware.os.miru.cluster.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.http.client.HttpClientException;
import com.jivesoftware.os.jive.utils.http.client.HttpResponse;
import com.jivesoftware.os.jive.utils.http.client.rest.ResponseMapper;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruStats;
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
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantAwareHttpClient;
import java.util.List;

public class MiruHttpClusterClient implements MiruClusterClient {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruStats miruStats;
    private final String routingTenantId;
    private final TenantAwareHttpClient<String> client;
    private final ObjectMapper requestMapper;
    private final ResponseMapper responseMapper;

    public MiruHttpClusterClient(MiruStats miruStats,
        String routingTenantId,
        TenantAwareHttpClient<String> client,
        ObjectMapper requestMapper,
        ResponseMapper responseMapper) {

        this.miruStats = miruStats;
        this.routingTenantId = routingTenantId;
        this.client = client;
        this.requestMapper = requestMapper;
        this.responseMapper = responseMapper;
    }

    private <R> R send(HttpCallable<R> send) {
        try {
            return send.call(client);
        } catch (Exception x) {
            LOG.warn("Failed to send " + send, x);
        }
        throw new RuntimeException("Failed to send.");
    }

    static interface HttpCallable<R> {

        R call(TenantAwareHttpClient<String> client) throws Exception;
    }

    @Override
    public MiruTopologyResponse routingTopology(final MiruTenantId tenantId) throws Exception {
        return send(new HttpCallable<MiruTopologyResponse>() {

            @Override
            public MiruTopologyResponse call(TenantAwareHttpClient<String> client) throws HttpClientException {
                HttpResponse response = client.get(routingTenantId,
                    "/miru/topology/routing/" + tenantId.toString());
                MiruTopologyResponse miruTopologyResponse = responseMapper.extractResultFromResponse(response, MiruTopologyResponse.class, null);
                miruStats.egressed("/miru/topology/routing/" + tenantId.toString(), 1);
                return miruTopologyResponse;
            }
        });
    }

    @Override
    public MiruHeartbeatResponse thumpthump(final MiruHost host, final MiruHeartbeatRequest heartbeatRequest) {
        return send(new HttpCallable<MiruHeartbeatResponse>() {

            @Override
            public MiruHeartbeatResponse call(TenantAwareHttpClient<String> client) throws Exception {
                String jsonHeartbeatRequest = requestMapper.writeValueAsString(heartbeatRequest);
                HttpResponse response = client.postJson(routingTenantId,
                    "/miru/topology/thumpthump/"
                    + host.getLogicalName() + "/"
                    + host.getPort(),
                    jsonHeartbeatRequest);
                MiruHeartbeatResponse heartbeatResponse = responseMapper.extractResultFromResponse(response, MiruHeartbeatResponse.class, null);
                miruStats.egressed("/miru/topology/thumpthump/"
                    + host.getLogicalName() + "/"
                    + host.getPort(), 1);
                return heartbeatResponse;
            }
        });
    }

    @Override
    public List<HostHeartbeat> allhosts() {
        return send(new HttpCallable<List<HostHeartbeat>>() {
            @Override
            public List<HostHeartbeat> call(TenantAwareHttpClient<String> client) throws Exception {
                HttpResponse response = client.postJson(routingTenantId,
                    "/miru/topology/allHosts",
                    "null");
                List<HostHeartbeat> heartBeats = responseMapper.extractResultFromResponse(response, List.class, new Class[]{HostHeartbeat.class}, null);
                miruStats.egressed("/miru/topology/allHosts", 1);
                return heartBeats;
            }
        });
    }

    @Override
    public MiruTenantConfig tenantConfig(final MiruTenantId tenantId) {
        return send(new HttpCallable<MiruTenantConfig>() {
            @Override
            public MiruTenantConfig call(TenantAwareHttpClient<String> client) throws Exception {
                HttpResponse response = client.get(routingTenantId,
                    "/miru/topology/tenantConfig/" + tenantId.toString());
                MiruTenantConfig tenantConfig = responseMapper.extractResultFromResponse(response, MiruTenantConfig.class, null);
                miruStats.egressed("/miru/topology/tenantConfig/" + tenantId.toString(), 1);
                return tenantConfig;
            }
        });
    }

    @Override
    public void elect(final MiruHost host, final MiruTenantId tenantId, final MiruPartitionId partitionId, final long electionId) {
        send(new HttpCallable<String>() {
            @Override
            public String call(TenantAwareHttpClient<String> client) throws Exception {
                HttpResponse response = client.postJson(routingTenantId,
                    "/miru/topology/elect/"
                    + host.getLogicalName() + "/"
                    + host.getPort() + "/"
                    + tenantId + "/"
                    + partitionId.getId() + "/"
                    + electionId, "null");

                String r = responseMapper.extractResultFromResponse(response, String.class, null);
                miruStats.egressed("/miru/topology/elect/"
                    + host.getLogicalName() + "/"
                    + host.getPort() + "/"
                    + tenantId + "/"
                    + partitionId.getId() + "/"
                    + electionId, 1);
                return r;
            }
        });
    }

    @Override
    public void removeReplica(final MiruTenantId tenantId, final MiruPartitionId partitionId) {
        send(new HttpCallable<String>() {
            @Override
            public String call(TenantAwareHttpClient<String> client) throws Exception {
                HttpResponse response = client.postJson(routingTenantId,
                    "/miru/topology/remove/replica/"
                    + tenantId + "/"
                    + partitionId.getId(), "null");
                String r = responseMapper.extractResultFromResponse(response, String.class, null);
                miruStats.egressed("/miru/topology/remove/replica/"
                    + tenantId + "/"
                    + partitionId.getId(), 1);
                return r;

            }
        });
    }

    @Override
    public List<MiruPartition> partitions(final MiruTenantId tenantId) {
        return send(new HttpCallable<List<MiruPartition>>() {
            @Override
            public List<MiruPartition> call(TenantAwareHttpClient<String> client) throws Exception {
                HttpResponse response = client.postJson(routingTenantId,
                    "/miru/topology/partitions/" + tenantId.toString(), "null");
                List<MiruPartition> partitions = responseMapper.extractResultFromResponse(response, List.class, new Class[]{MiruPartition.class}, null);
                miruStats.egressed("/miru/topology/partitions/" + tenantId.toString(), 1);
                return partitions;
            }
        });
    }

    @Override
    public MiruReplicaHosts replicas(final MiruTenantId tenantId, final MiruPartitionId partitionId) {
        return send(new HttpCallable<MiruReplicaHosts>() {
            @Override
            public MiruReplicaHosts call(TenantAwareHttpClient<String> client) throws Exception {
                HttpResponse response = client.get(routingTenantId,
                    "/miru/topology/replicas/" + tenantId.toString() + "/" + partitionId.getId());
                MiruReplicaHosts hosts = responseMapper.extractResultFromResponse(response, MiruReplicaHosts.class, null);
                miruStats.egressed("/miru/topology/replicas/" + tenantId.toString() + "/" + partitionId.getId(), 1);
                return hosts;
            }
        });
    }

    @Override
    public void remove(final MiruHost host) {
        send(new HttpCallable<String>() {
            @Override
            public String call(TenantAwareHttpClient<String> client) throws Exception {
                HttpResponse response = client.postJson(routingTenantId,
                    "/miru/topology/remove/"
                    + host.getLogicalName() + "/"
                    + host.getPort(), "null");
                String r = responseMapper.extractResultFromResponse(response, String.class, null);
                miruStats.egressed("/miru/topology/remove/"
                    + host.getLogicalName() + "/"
                    + host.getPort(), 1);
                return r;
            }
        });
    }

    @Override
    public void remove(final MiruHost host, final MiruTenantId tenantId, final MiruPartitionId partitionId) {
        send(new HttpCallable<String>() {
            @Override
            public String call(TenantAwareHttpClient<String> client) throws Exception {
                HttpResponse response = client.postJson(routingTenantId,
                    "/miru/topology/remove/"
                    + host.getLogicalName() + "/"
                    + host.getPort() + "/"
                    + tenantId + "/"
                    + partitionId.getId(), "null");
                String r = responseMapper.extractResultFromResponse(response, String.class, null);
                miruStats.egressed("/miru/topology/remove/"
                    + host.getLogicalName() + "/"
                    + host.getPort() + "/"
                    + tenantId + "/"
                    + partitionId.getId(), 1);
                return r;
            }
        });
    }

    @Override
    public MiruSchema getSchema(final MiruTenantId tenantId) {
        return send(new HttpCallable<MiruSchema>() {
            @Override
            public MiruSchema call(TenantAwareHttpClient<String> client) throws Exception {
                HttpResponse response = client.get(routingTenantId,
                    "/miru/topology/schema/" + tenantId.toString());
                MiruSchema schema = responseMapper.extractResultFromResponse(response, MiruSchema.class, null);
                miruStats.egressed("/miru/topology/schema/" + tenantId.toString(), 1);
                return schema;
            }
        });
    }

    @Override
    public void registerSchema(final MiruTenantId tenantId, final MiruSchema schema) {
        send(new HttpCallable<String>() {
            @Override
            public String call(TenantAwareHttpClient<String> client) throws Exception {
                String jsonSchema = requestMapper.writeValueAsString(schema);
                HttpResponse response = client.postJson(routingTenantId,
                    "/miru/topology/schema/" + tenantId.toString(), jsonSchema);
                String r = responseMapper.extractResultFromResponse(response, String.class, null);
                miruStats.egressed("/miru/topology/schema/" + tenantId.toString(), 1);
                return r;
            }
        });
    }
}
