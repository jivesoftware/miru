package com.jivesoftware.os.miru.stumptown.deployable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.logappender.MiruLogEvent;
import com.jivesoftware.os.miru.stumptown.deployable.storage.MiruStumptownPayloadStorage;
import com.jivesoftware.os.miru.stumptown.deployable.storage.MiruStumptownPayloadsAmza;
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
public class MiruStumptownIntakeService {

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
    private final StumptownSchemaService stumptownSchemaService;
    private final LogMill logMill;
    private final String miruIngressEndpoint;
    private final ObjectMapper activityMapper;
    private final TenantAwareHttpClient<String> miruWriter;
    private final MiruStumptownPayloadStorage payloads;
    private final RoundRobinStrategy roundRobinStrategy = new RoundRobinStrategy();

    public MiruStumptownIntakeService(boolean enabled,
        StumptownSchemaService stumptownSchemaService,
        LogMill logMill,
        String miruIngressEndpoint,
        ObjectMapper activityMapper,
        TenantAwareHttpClient<String> miruWriter,
        MiruStumptownPayloadStorage payloads) {
        this.enabled = enabled;
        this.stumptownSchemaService = stumptownSchemaService;
        this.logMill = logMill;
        this.miruIngressEndpoint = miruIngressEndpoint;
        this.activityMapper = activityMapper;
        this.miruWriter = miruWriter;
        this.payloads = payloads;
    }

    void ingressLogEvents(List<MiruLogEvent> logEvents) throws Exception {
        if (enabled) {
            stumptownSchemaService.ensureSchema(StumptownSchemaConstants.TENANT_ID, StumptownSchemaConstants.SCHEMA);

            List<MiruActivity> activities = Lists.newArrayListWithCapacity(logEvents.size());
            List<MiruStumptownPayloadsAmza.TimeAndPayload<MiruLogEvent>> timedLogEvents = Lists.newArrayListWithCapacity(logEvents.size());

            for (MiruLogEvent logEvent : logEvents) {
                MiruActivity activity = logMill.mill(StumptownSchemaConstants.TENANT_ID, logEvent);
                activities.add(activity);
                timedLogEvents.add(new MiruStumptownPayloadsAmza.TimeAndPayload<>(activity.time, logEvent));
            }

            record(timedLogEvents);
            ingress(activities);

            log.inc("ingressed", timedLogEvents.size());
            log.info("Ingressed " + timedLogEvents.size());
        } else {
            log.inc("ingressed>droppedOnFloor", logEvents.size());
            log.info("Ingressed dropped on floor:" + logEvents.size());
        }
    }

    private void ingress(List<MiruActivity> activities) throws JsonProcessingException {
        ingressLatency.startTimer();
        try {
            String jsonActivities = activityMapper.writeValueAsString(activities);
            while (true) {
                try {
                    // TODO expose "" tenant to config?
                    HttpResponse response = miruWriter.call("", roundRobinStrategy, "ingress",
                        client -> new ClientCall.ClientResponse<>(client.postJson(miruIngressEndpoint, jsonActivities, null), true));
                    if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                        throw new RuntimeException("Failed to post " + activities.size() + " to " + miruIngressEndpoint);
                    }
                    log.inc("ingressed");
                    break;
                } catch (Exception x) {
                    try {
                        log.error("Failed to forward ingress. Will retry shortly....", x);
                        Thread.sleep(5000);
                    } catch (InterruptedException ex) {
                        Thread.interrupted();
                        return;
                    }
                }
            }
        } finally {
            ingressLatency.stopTimer("Ingress " + activities.size(), "Add more stumptown services or fix down stream issue.");
        }
    }

    private void record(List<MiruStumptownPayloadsAmza.TimeAndPayload<MiruLogEvent>> timedLogEvents) throws Exception {
        payloads.multiPut(StumptownSchemaConstants.TENANT_ID, timedLogEvents);
    }
}
