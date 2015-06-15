package com.jivesoftware.os.miru.cluster.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;

public class MiruClusterClientInitializer {

    public MiruClusterClient initialize(MiruStats miruStats, String routingTenantId, TenantAwareHttpClient<String> client, ObjectMapper mapper)
        throws Exception {

        return new MiruHttpClusterClient(miruStats, routingTenantId, client, new RoundRobinStrategy(), mapper, new HttpResponseMapper(mapper));
    }
}
