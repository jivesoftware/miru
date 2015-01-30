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
package com.jivesoftware.os.miru.lumberyard.deployable;

import com.jivesoftware.os.jive.utils.health.api.HealthFactory;
import com.jivesoftware.os.jive.utils.health.api.HealthTimer;
import com.jivesoftware.os.jive.utils.health.api.TimerHealthCheckConfig;
import com.jivesoftware.os.jive.utils.health.checkers.TimerHealthChecker;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.logappender.MiruLogEvent;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.merlin.config.defaults.DoubleDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan.colt
 */
public class MiruLumberyardIntakeService {

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

    private final Random rand = new Random();
    private final LumberyardSchemaService lumberyardSchemaService;
    private final LogMill logMill;
    private final String miruIngressEndpoint;
    private final RequestHelper[] miruWriter;

    public MiruLumberyardIntakeService(LumberyardSchemaService lumberyardSchemaService,
        LogMill logMill,
        String miruIngressEndpoint,
        RequestHelper[] miruWrites) {
        this.lumberyardSchemaService = lumberyardSchemaService;
        this.logMill = logMill;
        this.miruIngressEndpoint = miruIngressEndpoint;
        this.miruWriter = miruWrites;
    }

    void ingressLogEvents(List<MiruLogEvent> logEvents) throws Exception {
        List<MiruActivity> miruActivites = new ArrayList<>();
        for (MiruLogEvent logEvent : logEvents) {
            MiruTenantId tenantId = LumberyardSchemaConstants.TENANT_ID;
            lumberyardSchemaService.ensureSchema(tenantId, LumberyardSchemaConstants.SCHEMA);
            miruActivites.add(logMill.mill(tenantId, logEvent));
        }
        ingress(miruActivites);
        log.inc("ingressed", miruActivites.size());
        log.info("Ingressed " + miruActivites.size());
    }

    void ingress(List<MiruActivity> miruActivites) {
        int index = 0;
        ingressLatency.startTimer();
        try {
            while (true) {
                try {
                    index = rand.nextInt(miruWriter.length);
                    RequestHelper requestHelper = miruWriter[index];
                    requestHelper.executeRequest(miruActivites, miruIngressEndpoint, String.class, null);
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
            ingressLatency.stopTimer("Ingress " + miruActivites.size(), "Add more lumberyard services or fix down stream issue.");
        }

    }
}
