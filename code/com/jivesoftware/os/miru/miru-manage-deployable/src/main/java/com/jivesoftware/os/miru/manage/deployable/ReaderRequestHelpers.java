package com.jivesoftware.os.miru.manage.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 *
 */
public class ReaderRequestHelpers {

    private final MiruClusterRegistry clusterRegistry;
    private final ObjectMapper objectMapper;

    private final Random random = new Random();
    private final HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
        .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList());

    public ReaderRequestHelpers(MiruClusterRegistry clusterRegistry, ObjectMapper objectMapper) {
        this.clusterRegistry = clusterRegistry;
        this.objectMapper = objectMapper;
    }

    public RequestHelper get(Optional<MiruHost> excludingHost) throws Exception {
        List<MiruHost> hosts = Lists.newArrayList();
        for (MiruClusterRegistry.HostHeartbeat heartbeat : clusterRegistry.getAllHosts()) {
            hosts.add(heartbeat.host);
        }
        if (excludingHost.isPresent()) {
            hosts.remove(excludingHost.get());
        }

        if (hosts.isEmpty()) {
            throw new RuntimeException("No hosts to complete request");
        }

        MiruHost host = hosts.get(random.nextInt(hosts.size()));
        return new RequestHelper(httpClientFactory.createClient(host.getLogicalName(), host.getPort()), objectMapper);
    }
}
