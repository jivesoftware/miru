package com.jivesoftware.os.miru.cluster.client;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.jivesoftware.os.miru.api.topology.MiruIngressUpdate;
import com.jivesoftware.os.miru.api.topology.MiruTenantConfig;
import com.jivesoftware.os.miru.api.topology.MiruTopologyResponse;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantAwareHttpClient;
import java.util.Collection;
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
        return send(client -> {
            long start = System.currentTimeMillis();
            HttpResponse response = client.get(routingTenantId,
                "/miru/topology/routing/" + tenantId.toString());
            MiruTopologyResponse miruTopologyResponse = responseMapper.extractResultFromResponse(response, MiruTopologyResponse.class, null);
            miruStats.egressed("/miru/topology/routing/" + tenantId.toString(), 1, System.currentTimeMillis() - start);
            return miruTopologyResponse;
        });
    }

    @Override
    public void updateIngress(Collection<MiruIngressUpdate> ingressUpdates) throws Exception {
        send(client -> {
            long start = System.currentTimeMillis();
            String jsonWarmIngress = requestMapper.writeValueAsString(ingressUpdates);
            HttpResponse response = client.postJson(routingTenantId,
                "/miru/topology/update/ingress",
                jsonWarmIngress);
            String r = responseMapper.extractResultFromResponse(response, String.class, null);
            miruStats.egressed("/miru/topology/update/ingress", 1, System.currentTimeMillis() - start);
            return r;
        });
    }

    @Override
    public MiruHeartbeatResponse thumpthump(final MiruHost host, final MiruHeartbeatRequest heartbeatRequest) {
        return send(client -> {
            long start = System.currentTimeMillis();
            String jsonHeartbeatRequest = requestMapper.writeValueAsString(heartbeatRequest);
            HttpResponse response = client.postJson(routingTenantId,
                "/miru/topology/thumpthump/"
                    + host.getLogicalName() + "/"
                    + host.getPort(),
                jsonHeartbeatRequest);
            MiruHeartbeatResponse heartbeatResponse = responseMapper.extractResultFromResponse(response, MiruHeartbeatResponse.class, null);
            miruStats.egressed("/miru/topology/thumpthump/"
                + host.getLogicalName() + "/"
                + host.getPort(), 1, System.currentTimeMillis() - start);
            return heartbeatResponse;
        });
    }

    @Override
    public List<HostHeartbeat> allhosts() {
        return send(client -> {
            long start = System.currentTimeMillis();
            HttpResponse response = client.postJson(routingTenantId,
                "/miru/topology/allHosts",
                "null");
            List<HostHeartbeat> heartBeats = responseMapper.extractResultFromResponse(response, List.class, new Class[] { HostHeartbeat.class }, null);
            miruStats.egressed("/miru/topology/allHosts", 1, System.currentTimeMillis() - start);
            return heartBeats;
        });
    }

    @Override
    public MiruTenantConfig tenantConfig(final MiruTenantId tenantId) {
        return send(client -> {
            long start = System.currentTimeMillis();
            HttpResponse response = client.get(routingTenantId,
                "/miru/topology/tenantConfig/" + tenantId.toString());
            MiruTenantConfig tenantConfig = responseMapper.extractResultFromResponse(response, MiruTenantConfig.class, null);
            miruStats.egressed("/miru/topology/tenantConfig/" + tenantId.toString(), 1, System.currentTimeMillis() - start);
            return tenantConfig;
        });
    }

    @Override
    public List<MiruPartition> partitions(final MiruTenantId tenantId) {
        return send(client -> {
            long start = System.currentTimeMillis();
            HttpResponse response = client.postJson(routingTenantId,
                "/miru/topology/partitions/" + tenantId.toString(), "null");
            List<MiruPartition> partitions = responseMapper.extractResultFromResponse(response, List.class, new Class[] { MiruPartition.class }, null);
            miruStats.egressed("/miru/topology/partitions/" + tenantId.toString(), 1, System.currentTimeMillis() - start);
            return partitions;
        });
    }

    @Override
    public void remove(final MiruHost host) {
        send(client -> {
            long start = System.currentTimeMillis();
            HttpResponse response = client.postJson(routingTenantId,
                "/miru/topology/remove/"
                    + host.getLogicalName() + "/"
                    + host.getPort(), "null");
            String r = responseMapper.extractResultFromResponse(response, String.class, null);
            miruStats.egressed("/miru/topology/remove/"
                + host.getLogicalName() + "/"
                + host.getPort(), 1, System.currentTimeMillis() - start);
            return r;
        });
    }

    @Override
    public void remove(final MiruHost host, final MiruTenantId tenantId, final MiruPartitionId partitionId) {
        send(client -> {
            long start = System.currentTimeMillis();
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
                + partitionId.getId(), 1, System.currentTimeMillis() - start);
            return r;
        });
    }

    @Override
    public MiruSchema getSchema(final MiruTenantId tenantId) {
        return send(client -> {
            long start = System.currentTimeMillis();
            HttpResponse response = client.get(routingTenantId,
                "/miru/topology/schema/" + tenantId.toString());
            MiruSchema schema = responseMapper.extractResultFromResponse(response, MiruSchema.class, null);
            miruStats.egressed("/miru/topology/schema/" + tenantId.toString(), 1, System.currentTimeMillis() - start);
            return schema;
        });
    }

    @Override
    public void registerSchema(final MiruTenantId tenantId, final MiruSchema schema) {
        send(client -> {
            long start = System.currentTimeMillis();
            String jsonSchema = requestMapper.writeValueAsString(schema);
            HttpResponse response = client.postJson(routingTenantId,
                "/miru/topology/schema/" + tenantId.toString(), jsonSchema);
            String r = responseMapper.extractResultFromResponse(response, String.class, null);
            miruStats.egressed("/miru/topology/schema/" + tenantId.toString(), 1, System.currentTimeMillis() - start);
            return r;
        });
    }
}
