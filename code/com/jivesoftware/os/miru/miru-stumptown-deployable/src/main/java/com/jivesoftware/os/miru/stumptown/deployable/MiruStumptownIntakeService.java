/*
 * Copyright 2015 jonathan.colt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.miru.stumptown.deployable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.health.api.HealthFactory;
import com.jivesoftware.os.jive.utils.health.api.HealthTimer;
import com.jivesoftware.os.jive.utils.health.api.TimerHealthCheckConfig;
import com.jivesoftware.os.jive.utils.health.checkers.TimerHealthChecker;
import com.jivesoftware.os.jive.utils.http.client.HttpResponse;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.logappender.MiruLogEvent;
import com.jivesoftware.os.miru.stumptown.deployable.storage.MiruStumptownPayloads;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantAwareHttpClient;
import java.util.List;
import org.merlin.config.defaults.DoubleDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan.colt
 */
public class MiruStumptownIntakeService {

    public static interface IngressLatency extends TimerHealthCheckConfig {

        @StringDefault("ingress>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to ingress batches of logEvents.")
        @Override
        String getDescription();

        @DoubleDefault(30000d) /// 30sec
        @Override
        Double get95ThPecentileMax();
    }

    private static final HealthTimer ingressLatency = HealthFactory.getHealthTimer(IngressLatency.class, TimerHealthChecker.FACTORY);
    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final StumptownSchemaService stumptownSchemaService;
    private final LogMill logMill;
    private final String miruIngressEndpoint;
    private final ObjectMapper activityMapper;
    private final TenantAwareHttpClient<String> miruWriter;
    private final MiruStumptownPayloads payloads;

    public MiruStumptownIntakeService(StumptownSchemaService stumptownSchemaService,
        LogMill logMill,
        String miruIngressEndpoint,
        ObjectMapper activityMapper,
        TenantAwareHttpClient<String> miruWriter,
        MiruStumptownPayloads payloads) {
        this.stumptownSchemaService = stumptownSchemaService;
        this.logMill = logMill;
        this.miruIngressEndpoint = miruIngressEndpoint;
        this.activityMapper = activityMapper;
        this.miruWriter = miruWriter;
        this.payloads = payloads;
    }

    void ingressLogEvents(List<MiruLogEvent> logEvents) throws Exception {
        List<MiruActivity> activities = Lists.newArrayListWithCapacity(logEvents.size());
        List<MiruStumptownPayloads.TimeAndPayload<MiruLogEvent>> timedLogEvents = Lists.newArrayListWithCapacity(logEvents.size());
        for (MiruLogEvent logEvent : logEvents) {
            MiruTenantId tenantId = StumptownSchemaConstants.TENANT_ID;
            stumptownSchemaService.ensureSchema(tenantId, StumptownSchemaConstants.SCHEMA);
            MiruActivity activity = logMill.mill(tenantId, logEvent);
            activities.add(activity);
            timedLogEvents.add(new MiruStumptownPayloads.TimeAndPayload<>(activity.time, logEvent));
        }
        ingress(activities);
        record(timedLogEvents);
        log.inc("ingressed", timedLogEvents.size());
        log.info("Ingressed " + timedLogEvents.size());
    }

    private void ingress(List<MiruActivity> activities) throws JsonProcessingException {
        ingressLatency.startTimer();
        try {
            String jsonActivities = activityMapper.writeValueAsString(activities);
            while (true) {
                try {
                    HttpResponse postJson = miruWriter.postJson("", miruIngressEndpoint, jsonActivities); // TODO expose "" tenant to config?
                    if (postJson.getStatusCode() < 200 || postJson.getStatusCode() >= 300) {
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

    private void record(List<MiruStumptownPayloads.TimeAndPayload<MiruLogEvent>> timedLogEvents) throws Exception {
        payloads.multiPut(StumptownSchemaConstants.TENANT_ID, timedLogEvents);
    }
}
