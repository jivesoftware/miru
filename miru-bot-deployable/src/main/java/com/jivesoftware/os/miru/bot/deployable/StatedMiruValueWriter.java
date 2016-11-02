package com.jivesoftware.os.miru.bot.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import com.jivesoftware.os.miru.bot.deployable.MiruBotDistinctsInitializer.MiruBotDistinctsConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

class StatedMiruValueWriter {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final String endpoint;
    private final MiruBotDistinctsConfig config;
    private final TenantAwareHttpClient<String> client;
    private final OrderIdProvider order;

    private ObjectMapper objectMapper = new ObjectMapper();
    private final NextClientStrategy nextClientStrategy = new RoundRobinStrategy();
    private final Random RAND = new Random();

    StatedMiruValueWriter(String endpoint,
                          MiruBotDistinctsConfig config,
                          TenantAwareHttpClient<String> client,
                          OrderIdProvider order) {
        this.endpoint = endpoint;
        this.config = config;
        this.client = client;
        this.order = order;
    }

    void write(MiruTenantId miruTenantId,
               List<Map<String, StatedMiruValue>> fieldsStatedValues) throws Exception {
        LOG.debug("Miru write {} activities", fieldsStatedValues.size());

        List<MiruActivity> miruActivities = Lists.newArrayList();
        for (Map<String, StatedMiruValue> fieldValue : fieldsStatedValues) {
            StringBuilder sb = new StringBuilder();
            Map<String, List<String>> fieldsValues = Maps.newHashMap();

            for (Map.Entry<String, StatedMiruValue> value : fieldValue.entrySet()) {
                sb.append(value.getKey());
                sb.append("-");
                sb.append(value.getValue().value.last());
                sb.append(",");

                fieldsValues.put(value.getKey(), Collections.singletonList(value.getValue().value.last()));
            }

            if (LOG.isDebugEnabled()) LOG.debug("Write miru activity: " + sb.toString());
            miruActivities.add(new MiruActivity(
                    miruTenantId,
                    order.nextId(),
                    0,
                    false,
                    new String[0],
                    fieldsValues,
                    Collections.emptyMap()));
        }

        String jsonActivities = objectMapper.writeValueAsString(miruActivities);
        LOG.debug("Miru activity json post data '{}'", jsonActivities);

        HttpResponse httpResponse = client.call(
                "",
                nextClientStrategy,
                "ingress",
                client -> new ClientCall.ClientResponse<>(client.postJson(endpoint, jsonActivities, null), true));

        LOG.debug("Miru write response {}:{}", httpResponse.getStatusCode(), new String(httpResponse.getResponseBody()));
        if (httpResponse.getStatusCode() < 200 || httpResponse.getStatusCode() >= 300) {
            throw new RuntimeException("Failed to post activities to " + endpoint);
        }

        for (Map<String, StatedMiruValue> fieldValue : fieldsStatedValues) {
            for (Map.Entry<String, StatedMiruValue> value : fieldValue.entrySet()) {
                value.getValue().state = StatedMiruValue.State.WRITTEN;
            }
        }
    }

    int writeAll(MiruBotBucket miruBotBucket,
                 MiruTenantId miruTenantId) throws Exception {
        int res = 0;
        while (res < config.getReadFrequency()) {
            List<Map<String, StatedMiruValue>> fieldsValues = Lists.newArrayList();

            int activityCount = 1;
            if ((res + 1) % config.getBatchWriteFrequency() == 0) {
                activityCount += RAND.nextInt(config.getBatchWriteCountFactor());
            }

            for (int j = 0; j < activityCount; j++) {
                fieldsValues.add(miruBotBucket.genWriteMiruActivity());
                res++;
            }

            write(miruTenantId, fieldsValues);
            LOG.debug("Wrote {} activity batch.", fieldsValues.size());

            if (config.getWriteHesitationFactorMs() > 0) {
                Thread.sleep(RAND.nextInt(config.getWriteHesitationFactorMs()));
            }
        }

        return res;
    }

}
