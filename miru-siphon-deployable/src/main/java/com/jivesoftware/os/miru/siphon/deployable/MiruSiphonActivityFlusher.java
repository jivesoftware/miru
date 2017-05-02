package com.jivesoftware.os.miru.siphon.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.query.siphon.MiruSiphonPlugin;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import java.util.Collection;

/**
 * Created by jonathan.colt on 4/28/17.
 */
public class MiruSiphonActivityFlusher {

    public static final MetricLogger LOG = MetricLoggerFactory.getLogger();


    private final RoundRobinStrategy robinStrategy = new RoundRobinStrategy(); // TODO tail at scale?


    private final MiruSiphonSchemaService miruSiphonSchemaService;
    private final String miruIngressEndpoint;
    private final ObjectMapper mapper;
    private final TenantAwareHttpClient<String> miruWriter;

    public MiruSiphonActivityFlusher(MiruSiphonSchemaService miruSiphonSchemaService,
        String miruIngressEndpoint,
        ObjectMapper mapper,
        TenantAwareHttpClient<String> miruWriter) {
        this.miruSiphonSchemaService = miruSiphonSchemaService;
        this.miruIngressEndpoint = miruIngressEndpoint;
        this.mapper = mapper;
        this.miruWriter = miruWriter;
    }

    public void flushActivities(MiruSiphonPlugin miruSiphonPlugin, MiruTenantId tenantId, Collection<MiruActivity> activities) throws Exception {

        if (!miruSiphonSchemaService.ensured(tenantId)) {
            miruSiphonSchemaService.ensureSchema(tenantId, miruSiphonPlugin.schema(tenantId));
        }

        String jsonActivities = mapper.writeValueAsString(activities);
        while (true && !activities.isEmpty()) {
            try {
                HttpResponse response = miruWriter.call("", robinStrategy, "flushActivities",
                    client -> new ClientCall.ClientResponse<>(client.postJson(miruIngressEndpoint, jsonActivities, null), true));
                if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                    throw new RuntimeException("Failed to post " + activities.size() + " to " + miruIngressEndpoint);
                }
                LOG.inc("flushed");
                break;
            } catch (Exception x) {
                try {
                    LOG.error("Failed to flush activities. Will retry shortly....", x);
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                    Thread.interrupted();
                    return;
                }
            }
        }
    }
}
