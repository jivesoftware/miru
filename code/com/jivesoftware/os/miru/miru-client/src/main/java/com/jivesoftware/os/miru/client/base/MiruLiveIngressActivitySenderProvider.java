package com.jivesoftware.os.miru.client.base;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruWriter;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.client.MiruActivitySenderProvider;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/** @author jonathan */
public class MiruLiveIngressActivitySenderProvider implements MiruActivitySenderProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ObjectMapper objectMapper;
    private final String sendActivitesEndpointUrl;
    private final HttpClientFactory httpClientFactory;

    private static final ConcurrentHashMap<String, RequestHelper> hostToRequestHelpers = new ConcurrentHashMap<>();

    public MiruLiveIngressActivitySenderProvider(HttpClientFactory httpClientFactory,
        ObjectMapper objectMapper) {
        this.sendActivitesEndpointUrl = MiruWriter.WRITER_SERVICE_ENDPOINT_PREFIX + MiruWriter.ADD_ACTIVITIES;
        this.httpClientFactory = httpClientFactory;
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
        HttpClient httpClient = httpClientFactory.createClient(host.getLogicalName(), host.getPort());
        return new RequestHelper(httpClient, objectMapper);
    }
}
