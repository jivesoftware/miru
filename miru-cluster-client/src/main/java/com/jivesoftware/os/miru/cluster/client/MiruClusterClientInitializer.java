package com.jivesoftware.os.miru.cluster.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.TailAtScaleStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import java.util.Map;
import java.util.concurrent.Executor;

public class MiruClusterClientInitializer {


    private final Executor executor;
    private final int windowSize;
    private final float percentile;
    private final long initialSLAMillis;

    private final Map<String, TailAtScaleStrategy> tenantNextClientStrategy = Maps.newConcurrentMap();

    public MiruClusterClientInitializer(Executor executor,
        int windowSize,
        float percentile,
        long initialSLAMillis) {

        this.executor = executor;
        this.windowSize = windowSize;
        this.percentile = percentile;
        this.initialSLAMillis = initialSLAMillis;
    }

    public MiruClusterClient initialize(MiruStats miruStats, String routingTenantId, TenantAwareHttpClient<String> client, ObjectMapper mapper)
        throws Exception {


        TailAtScaleStrategy nextClientStrategy = tenantNextClientStrategy.computeIfAbsent(routingTenantId,
            (key) -> new TailAtScaleStrategy(executor, windowSize, percentile, initialSLAMillis));

        return new MiruHttpClusterClient(miruStats, routingTenantId, client, nextClientStrategy, mapper, new HttpResponseMapper(mapper));
    }
}
