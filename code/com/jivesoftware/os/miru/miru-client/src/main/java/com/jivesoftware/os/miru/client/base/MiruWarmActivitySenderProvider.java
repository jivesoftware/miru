package com.jivesoftware.os.miru.client.base;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.client.MiruActivitySenderProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** @author jonathan */
public class MiruWarmActivitySenderProvider implements MiruActivitySenderProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ObjectMapper objectMapper;
    private final String warmAllEndpointUrl;
    private final HttpClientFactory httpClientFactory;

    private static final ConcurrentHashMap<String, RequestHelper> hostToRequestHelpers = new ConcurrentHashMap<>();

    public MiruWarmActivitySenderProvider(HttpClientFactory httpClientFactory,
        ObjectMapper objectMapper) {
        this.warmAllEndpointUrl = MiruReader.QUERY_SERVICE_ENDPOINT_PREFIX + MiruReader.WARM_ALL_ENDPOINT;
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
                Set<MiruTenantId> tenantIds = Sets.newHashSet();
                for (MiruPartitionedActivity activity : activities) {
                    tenantIds.add(activity.tenantId);
                }

                try {
                    requestHelper.executeRequest(Lists.newArrayList(tenantIds), warmAllEndpointUrl, String.class, null);

                    LOG.inc("warm", activities.size());
                    for (MiruTenantId tenantId : tenantIds) {
                        LOG.inc("warm", 1, tenantId.toString());
                    }
                } catch (Exception x) {
                    LOG.warn("Failed to warm tenants {} for host: {}", new Object[] { tenantIds, host }, x);
                }
            }
        };
    }

    private RequestHelper create(MiruHost host) {
        HttpClient httpClient = httpClientFactory.createClient(host.getLogicalName(), host.getPort());
        return new RequestHelper(httpClient, objectMapper);
    }
}
