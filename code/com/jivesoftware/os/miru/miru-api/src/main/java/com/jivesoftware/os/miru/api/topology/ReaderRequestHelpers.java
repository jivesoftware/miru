package com.jivesoftware.os.miru.api.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruHost;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class ReaderRequestHelpers {

    private final MiruClusterClient clusterClient;
    private final ObjectMapper objectMapper;

    private final Random random = new Random();
    private final ConcurrentMap<MiruHost, RequestHelper> hostToHelper = Maps.newConcurrentMap();
    private final HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
        .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList());
    private final long ignoreHostThatHaveNotHeartBeatedInMillis;

    public ReaderRequestHelpers(MiruClusterClient clusterClient, ObjectMapper objectMapper, long ignoreHostThatHaveNotHeartBeatedInMillis) {
        this.clusterClient = clusterClient;
        this.objectMapper = objectMapper;
        this.ignoreHostThatHaveNotHeartBeatedInMillis = ignoreHostThatHaveNotHeartBeatedInMillis;
    }

    public RequestHelper get(MiruHost host) throws Exception {
        RequestHelper requestHelper = hostToHelper.get(host);
        if (requestHelper == null) {
            requestHelper = new RequestHelper(httpClientFactory.createClient(host.getLogicalName(), host.getPort()), objectMapper);
            RequestHelper existing = hostToHelper.putIfAbsent(host, requestHelper);
            if (existing != null) {
                requestHelper = existing;
            }
        }
        return requestHelper;
    }

    public List<RequestHelper> get(Optional<MiruHost> excludingHost) throws Exception {
        List<MiruHost> hosts = Lists.newArrayList();
        for (HostHeartbeat heartbeat : clusterClient.allhosts()) {
            if (heartbeat.heartbeat > (System.currentTimeMillis() - ignoreHostThatHaveNotHeartBeatedInMillis)) {
                hosts.add(heartbeat.host);
            }
        }
        if (excludingHost.isPresent()) {
            hosts.remove(excludingHost.get());
        }

        if (hosts.isEmpty()) {
            throw new RuntimeException("No hosts to complete request");
        }

        List<RequestHelper> requestHelpers = Lists.newArrayListWithCapacity(hosts.size());
        for (MiruHost host : hosts) {
            requestHelpers.add(get(host));
        }

        Collections.shuffle(requestHelpers, random);
        return requestHelpers;
    }
}
