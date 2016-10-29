package com.jivesoftware.os.miru.bot.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.miru.bot.deployable.MiruBotUniquesInitializer.MiruBotUniquesConfig;

class MiruBotUniquesService implements MiruBotHealthPercent {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruBotUniquesConfig miruBotUniquesConfig;
    private final String miruIngressEndpoint;
    private final ObjectMapper objectMapper;
    private final HttpResponseMapper httpResponseMapper;
    private final OrderIdProvider orderIdProvider;
    private final MiruBotSchemaService miruBotSchemaService;
    private final TenantAwareHttpClient<String> miruReader;
    private final TenantAwareHttpClient<String> miruWriter;

    private final RoundRobinStrategy roundRobinStrategy = new RoundRobinStrategy();

    MiruBotUniquesService(MiruBotUniquesConfig miruBotUniquesConfig,
                          String miruIngressEndpoint,
                          ObjectMapper objectMapper,
                          HttpResponseMapper httpResponseMapper,
                          OrderIdProvider orderIdProvider,
                          MiruBotSchemaService miruBotSchemaService,
                          TenantAwareHttpClient<String> miruReader,
                          TenantAwareHttpClient<String> miruWriter) {
        this.miruBotUniquesConfig = miruBotUniquesConfig;
        this.miruIngressEndpoint = miruIngressEndpoint;
        this.objectMapper = objectMapper;
        this.httpResponseMapper = httpResponseMapper;
        this.orderIdProvider = orderIdProvider;
        this.miruBotSchemaService = miruBotSchemaService;
        this.miruReader = miruReader;
        this.miruWriter = miruWriter;
    }

    void start() throws Exception {
        LOG.info("start");

        try {
            LOG.info("Enabled: " + miruBotUniquesConfig.getEnabled());
        } catch (Exception e) {
            LOG.error("Exception: " + e);
        }
    }

    public double getHealthPercentage() {
        return 1.0;
    }

    public String getHealthDescription() {
        return "";
    }

}
