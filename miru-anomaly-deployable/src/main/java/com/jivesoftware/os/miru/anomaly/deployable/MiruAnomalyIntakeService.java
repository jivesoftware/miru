package com.jivesoftware.os.miru.anomaly.deployable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.metric.sampler.AnomalyMetric;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.api.HealthTimer;
import com.jivesoftware.os.routing.bird.health.api.TimerHealthCheckConfig;
import com.jivesoftware.os.routing.bird.health.checkers.TimerHealthChecker;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import java.util.List;
import org.merlin.config.defaults.DoubleDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan.colt
 */
public class MiruAnomalyIntakeService {

    public interface IngressLatency extends TimerHealthCheckConfig {
        @StringDefault("ingress>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to ingress batches of logEvents.")
        @Override
        String getDescription();

        @DoubleDefault(30000d) /// 30sec
        @Override
        Double get95ThPercentileMax();
    }

    private static final HealthTimer ingressLatency = HealthFactory.getHealthTimer(IngressLatency.class, TimerHealthChecker.FACTORY);
    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final boolean enabled;
    private final AnomalySchemaService anomalySchemaService;
    private final SampleTrawl logMill;
    private final String miruIngressEndpoint;
    private final ObjectMapper activityMapper;
    private final TenantAwareHttpClient<String> miruWriterClient;
    private final RoundRobinStrategy roundRobinStrategy = new RoundRobinStrategy();

    public MiruAnomalyIntakeService(boolean enabled,
        AnomalySchemaService anomalySchemaService,
        SampleTrawl logMill,
        String miruIngressEndpoint,
        ObjectMapper activityMapper,
        TenantAwareHttpClient<String> miruWriterClient) {

        this.enabled = enabled;
        this.anomalySchemaService = anomalySchemaService;
        this.logMill = logMill;
        this.miruIngressEndpoint = miruIngressEndpoint;
        this.activityMapper = activityMapper;
        this.miruWriterClient = miruWriterClient;
    }

    void ingressEvents(List<AnomalyMetric> events) throws Exception {
        if (enabled) {
            List<MiruActivity> activities = Lists.newArrayListWithCapacity(events.size());
            for (AnomalyMetric logEvent : events) {
                MiruTenantId tenantId = AnomalySchemaConstants.TENANT_ID;
                anomalySchemaService.ensureSchema(tenantId, AnomalySchemaConstants.SCHEMA);
                MiruActivity activity = logMill.trawl(tenantId, logEvent);
                if (activity != null) {
                    activities.add(activity);
                }
            }
            if (!activities.isEmpty()) {
                ingress(activities);
                log.inc("ingressed", activities.size());
                log.info("Ingressed " + activities.size());
            }
        } else {
            log.inc("ingressed>droppedOnFloor", events.size());
            log.info("Ingressed dropped on floor" + events.size());
        }
    }

    private void ingress(List<MiruActivity> activities) throws JsonProcessingException {
        int index = 0;
        ingressLatency.startTimer();
        try {
            String jsonActivities = activityMapper.writeValueAsString(activities);
            while (true) {
                try {
                    // TODO expose "" tenant to config?
                    HttpResponse response = miruWriterClient.call("", roundRobinStrategy, "ingress",
                        client -> new ClientCall.ClientResponse<>(client.postJson(miruIngressEndpoint, jsonActivities, null), true));
                    if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                        throw new RuntimeException("Failed to post " + activities.size() + " to " + miruIngressEndpoint);
                    }
                    log.inc("ingressed");
                    break;
                } catch (Exception x) {
                    try {
                        log.error("Failed to forward ingress to miru at index=" + index + ". Will retry shortly....", x);
                        Thread.sleep(5000);
                    } catch (InterruptedException ex) {
                        Thread.interrupted();
                        return;
                    }
                }
            }
        } finally {
            ingressLatency.stopTimer("Ingress " + activities.size(), "Add more anomaly services or fix down stream issue.");
        }
    }

}
