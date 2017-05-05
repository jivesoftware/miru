package com.jivesoftware.os.miru.plugin.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruActorId;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruHostProvider;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolution;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.TailAtScaleStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptor;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
import com.jivesoftware.os.routing.bird.shared.InstanceDescriptor;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 *
 */
public class MiruQueryTASRouting implements MiruRouting {

    private final Map<MiruTenantIdAndFamily, TailAtScaleStrategy> strategyCache = Maps.newConcurrentMap();

    private final TenantAwareHttpClient<String> readerClient;
    private final ObjectMapper requestMapper;
    private final HttpResponseMapper responseMapper;
    private final Executor executor;
    private final int windowSize;
    private final float percentile;
    private final long initialSLAMillis;
    private final MiruQueryEvent queryEvent;

    public MiruQueryTASRouting(
        TenantAwareHttpClient<String> readerClient,
        ObjectMapper requestMapper,
        HttpResponseMapper responseMapper,
        Executor executor,
        int windowSize,
        float percentile,
        long initialSLAMillis,
        MiruQueryEvent queryEvent) {

        this.readerClient = readerClient;
        this.requestMapper = requestMapper;
        this.responseMapper = responseMapper;

        this.executor = executor;
        this.windowSize = windowSize;
        this.percentile = percentile;
        this.initialSLAMillis = initialSLAMillis;
        this.queryEvent = queryEvent;
    }


    @Override
    public <Q, A> MiruResponse<A> query(String routingTenant,
        String family,
        MiruRequest<Q> request,
        String path,
        Class<A> answerClass) throws Exception {

        MiruTenantIdAndFamily tenantAndFamily = new MiruTenantIdAndFamily(request.tenantId, family);
        long start = System.currentTimeMillis();
        try {
            String json = requestMapper.writeValueAsString(request);
            TailAtScaleStrategy tenantStrategy = getTenantStrategy(tenantAndFamily);
            InterceptingNextClientStrategy interceptingNextClientStrategy = new InterceptingNextClientStrategy(tenantStrategy);
            HttpResponse httpResponse = readerClient.call(routingTenant,
                interceptingNextClientStrategy,
                family,
                (c) -> new ClientCall.ClientResponse<>(c.postJson(path, json, null), true)
            );
            MiruResponse<A> answer = responseMapper.extractResultFromResponse(httpResponse, MiruResponse.class, new Class[] { answerClass }, null);
            recordTenantStrategy(tenantAndFamily, request.actorId, interceptingNextClientStrategy, answer);
            return answer;
        } catch (HttpClientException x) {
            queryEvent.event(tenantAndFamily.miruTenantId, request.actorId, tenantAndFamily.family, "nil", System.currentTimeMillis() - start, "failure");
            throw x;
        }
    }

    private TailAtScaleStrategy getTenantStrategy(MiruTenantIdAndFamily miruTenantIdAndFamily) {
        return strategyCache.getOrDefault(miruTenantIdAndFamily,
            new TailAtScaleStrategy(executor, windowSize, percentile, initialSLAMillis)
        );
    }


    private void recordTenantStrategy(MiruTenantIdAndFamily tenantAndFamily,
        MiruActorId actorId,
        InterceptingNextClientStrategy interceptingNextClientStrategy,
        MiruResponse<?> response) {

        if (response != null && response.solutions != null && !response.solutions.isEmpty()) {
            MiruSolution solution = response.solutions.get(0);
            MiruHost host = solution.usedPartition.host;

            ConnectionDescriptor favored = interceptingNextClientStrategy.favoredConnectionDescriptor;
            if (favored != null) {
                InstanceDescriptor instanceDescriptor = favored.getInstanceDescriptor();
                queryEvent.event(tenantAndFamily.miruTenantId, actorId, tenantAndFamily.family, instanceDescriptor.instanceKey, solution.totalElapsed,
                    "success",
                    "attempt:" + interceptingNextClientStrategy.attempt,
                    "totalAttempts:" + interceptingNextClientStrategy.totalAttempts,
                    "destinationService:" + instanceDescriptor.serviceName,
                    "destinationAddress:" + instanceDescriptor.publicHost + ":" + instanceDescriptor.ports.get("main")
                );
            }

            if (interceptingNextClientStrategy.favoredConnectionDescriptor != null) {
                HostPort hostPort = interceptingNextClientStrategy.favoredConnectionDescriptor.getHostPort();
                InstanceDescriptor instanceDescriptor = interceptingNextClientStrategy.favoredConnectionDescriptor.getInstanceDescriptor();
                if (!MiruHostProvider.checkEquals(host,
                    instanceDescriptor.instanceName, instanceDescriptor.instanceKey,
                    hostPort.getHost(), hostPort.getPort())) {

                    for (ConnectionDescriptor connectionDescriptor : interceptingNextClientStrategy.connectionDescriptors) {
                        hostPort = connectionDescriptor.getHostPort();
                        instanceDescriptor = connectionDescriptor.getInstanceDescriptor();
                        if (MiruHostProvider.checkEquals(host,
                            instanceDescriptor.instanceName, instanceDescriptor.instanceKey,
                            hostPort.getHost(), hostPort.getPort())) {

                            ((TailAtScaleStrategy) interceptingNextClientStrategy.delegate).favor(connectionDescriptor);

                            break;
                        }
                    }
                }
            }
        }
    }
}
