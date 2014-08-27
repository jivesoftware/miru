package com.jivesoftware.os.miru.client.base;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfig;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruWriter;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.client.MiruActivitySenderProvider;
import com.jivesoftware.os.miru.client.MiruClientConfig;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;

/** @author jonathan */
public class MiruHttpActivitySenderProvider implements MiruActivitySenderProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ObjectMapper objectMapper;
    private final String sendActivitesEndpointUrl;
    private final int socketTimeoutInMillis;
    private final int maxConnections; //  Not to be confused with naxConnections!

    private static final ConcurrentHashMap<String, RequestHelper> hostToRequestHelpers = new ConcurrentHashMap<>();

    @Inject
    public MiruHttpActivitySenderProvider(MiruClientConfig config, ObjectMapper objectMapper) {
        this.sendActivitesEndpointUrl = MiruWriter.WRITER_SERVICE_ENDPOINT_PREFIX + MiruWriter.ADD_ACTIVITIES;
        this.socketTimeoutInMillis = config.getSocketTimeoutInMillis();
        this.maxConnections = config.getMaxConnections();
        this.objectMapper = objectMapper;
    }

    @Override
    public MiruActivitySender get(final MiruHost host) {
        String key = host.toStringForm();
        RequestHelper got = hostToRequestHelpers.get(key);
        if (got == null) {
            got = create(host);
            RequestHelper had = hostToRequestHelpers.putIfAbsent(key, got);
            if (had != null) {
                got = had;
            }
        }
        final RequestHelper requestHelper = got;
        return new MiruActivitySender() {

            @Override
            public void send(List<MiruPartitionedActivity> activities) {
                try {
                    requestHelper.executeRequest(activities, sendActivitesEndpointUrl, String.class, null);
                } catch (Exception x) {
                    LOG.warn("Failed to send {} activities to host:{}", new Object[] { activities.size(), host });
                }
            }
        };
    }

    private RequestHelper create(MiruHost host) {
        Collection<HttpClientConfiguration> configurations = Lists.newArrayList();
        HttpClientConfig baseConfig = HttpClientConfig.newBuilder() // TODO refator so this is passed in.
            .setSocketTimeoutInMillis(socketTimeoutInMillis)
            .setMaxConnections(maxConnections)
            .build();
        configurations.add(baseConfig);
        HttpClientFactory createHttpClientFactory = new HttpClientFactoryProvider().createHttpClientFactory(configurations);
        HttpClient httpClient = createHttpClientFactory.createClient(host.getLogicalName(), host.getPort());
        return new RequestHelper(httpClient, objectMapper);
    }
}
