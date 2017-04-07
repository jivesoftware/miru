package com.jivesoftware.os.miru.plugin.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.TailAtScaleStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 *
 */
public class MiruTenantQueryRouting {

    private final Map<MiruTenantId, NextClientStrategy> strategyCache = Maps.newConcurrentMap();

    private final TenantAwareHttpClient<String> readerClient;
    private final ObjectMapper requestMapper;
    private final HttpResponseMapper responseMapper;
    private final Executor executor;
    private final int windowSize;
    private final float percentile;
    private final long initialSLAMillis;

    public MiruTenantQueryRouting(TenantAwareHttpClient<String> readerClient,
        ObjectMapper requestMapper,
        HttpResponseMapper responseMapper,
        Executor executor,
        int windowSize,
        float percentile,
        long initialSLAMillis) {
        this.readerClient = readerClient;
        this.requestMapper = requestMapper;
        this.responseMapper = responseMapper;

        this.executor = executor;
        this.windowSize = windowSize;
        this.percentile = percentile;
        this.initialSLAMillis = initialSLAMillis;
    }

    public <Q, A> MiruResponse<A> query(String routingTenant,
        String family,
        MiruRequest<Q> request,
        String path,
        Class<A> answerClass) throws Exception {

        String json = requestMapper.writeValueAsString(request);
        HttpResponse httpResponse = readerClient.call(routingTenant,
            getTenantStrategy(request.tenantId),
            family,
            (c) -> new ClientCall.ClientResponse<>(c.postJson(path, json, null), true)
        );
        return responseMapper.extractResultFromResponse(httpResponse, MiruResponse.class, new Class[] { answerClass }, null);
    }

    private NextClientStrategy getTenantStrategy(MiruTenantId tenantId) {
        return strategyCache.getOrDefault(tenantId, new TailAtScaleStrategy(executor, windowSize, percentile, initialSLAMillis));
    }

}
