package com.jivesoftware.os.miru.plugin.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruHostProvider;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.solution.MiruRequest;
import com.jivesoftware.os.miru.plugin.solution.MiruResponse;
import com.jivesoftware.os.miru.plugin.solution.MiruSolution;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TailAtScaleStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.ClientHealth;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptor;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
import com.jivesoftware.os.routing.bird.shared.IndexedClientStrategy;
import com.jivesoftware.os.routing.bird.shared.InstanceDescriptor;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import com.jivesoftware.os.routing.bird.shared.ReturnFirstNonFailure;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
    private final boolean tasEnabled;

    public MiruTenantQueryRouting(TenantAwareHttpClient<String> readerClient,
        ObjectMapper requestMapper,
        HttpResponseMapper responseMapper,
        Executor executor,
        int windowSize,
        float percentile,
        long initialSLAMillis,
        boolean tasEnabled) {

        this.readerClient = readerClient;
        this.requestMapper = requestMapper;
        this.responseMapper = responseMapper;

        this.executor = executor;
        this.windowSize = windowSize;
        this.percentile = percentile;
        this.initialSLAMillis = initialSLAMillis;
        this.tasEnabled = tasEnabled;
    }


    public <Q, A> MiruResponse<A> query(String routingTenant,
        String family,
        MiruRequest<Q> request,
        String path,
        Class<A> answerClass) throws Exception {

        String json = requestMapper.writeValueAsString(request);
        NextClientStrategy tenantStrategy = getTenantStrategy(request.tenantId);

        InterceptingNextClientStrategy interceptingNextClientStrategy = null;
        if (tasEnabled) {
            interceptingNextClientStrategy = new InterceptingNextClientStrategy((TailAtScaleStrategy) tenantStrategy);
            tenantStrategy = interceptingNextClientStrategy;
        }
        HttpResponse httpResponse = readerClient.call(routingTenant,
            tenantStrategy,
            family,
            (c) -> new ClientCall.ClientResponse<>(c.postJson(path, json, null), true)
        );
        MiruResponse<A> answer = responseMapper.extractResultFromResponse(httpResponse, MiruResponse.class, new Class[] { answerClass }, null);
        recordTenantStrategy(request.tenantId, interceptingNextClientStrategy, answer);
        return answer;
    }

    private static final class InterceptingNextClientStrategy implements NextClientStrategy {
        final TailAtScaleStrategy delegate;
        ConnectionDescriptor[] connectionDescriptors;
        ConnectionDescriptor favored;
        long latency;

        private InterceptingNextClientStrategy(TailAtScaleStrategy delegate) {
            this.delegate = delegate;
        }

        @Override
        public <C, R> R call(String family,
            ClientCall<C, R, HttpClientException> httpCall,
            ConnectionDescriptor[] connectionDescriptors,
            long connectionDescriptorsVersion,
            C[] clients,
            ClientHealth[] clientHealths,
            int deadAfterNErrors,
            long checkDeadEveryNMillis,
            AtomicInteger[] clientsErrors,
            AtomicLong[] clientsDeathTimestamp) throws HttpClientException {

            this.connectionDescriptors = connectionDescriptors;

            return delegate.call(family, httpCall, connectionDescriptors, connectionDescriptorsVersion, clients, clientHealths, deadAfterNErrors,
                checkDeadEveryNMillis, clientsErrors, clientsDeathTimestamp, (favored, latency) -> {
                    this.favored = favored;
                    this.latency = latency;
                });
        }
    }

    private NextClientStrategy getTenantStrategy(MiruTenantId tenantId) {
        return strategyCache.getOrDefault(tenantId,
            tasEnabled
                ? new TailAtScaleStrategy(executor, windowSize, percentile, initialSLAMillis)
                : new RoundRobinStrategy()
        );
    }


    private void recordTenantStrategy(MiruTenantId tenantId, InterceptingNextClientStrategy interceptingNextClientStrategy, MiruResponse<?> response) {
        if (!tasEnabled) {

            if (response != null && response.solutions != null && !response.solutions.isEmpty()) {
                MiruSolution solution = response.solutions.get(0);
                MiruHost host = solution.usedPartition.host;

                if (interceptingNextClientStrategy != null && interceptingNextClientStrategy.favored != null) {

                    HostPort hostPort = interceptingNextClientStrategy.favored.getHostPort();
                    InstanceDescriptor instanceDescriptor = interceptingNextClientStrategy.favored.getInstanceDescriptor();
                    if (!MiruHostProvider.checkEquals(host,
                        instanceDescriptor.instanceName, instanceDescriptor.instanceKey,
                        hostPort.getHost(), hostPort.getPort())) {

                        for (ConnectionDescriptor connectionDescriptor : interceptingNextClientStrategy.connectionDescriptors) {
                            hostPort = connectionDescriptor.getHostPort();
                            instanceDescriptor = connectionDescriptor.getInstanceDescriptor();
                            if (MiruHostProvider.checkEquals(host,
                                instanceDescriptor.instanceName, instanceDescriptor.instanceKey,
                                hostPort.getHost(), hostPort.getPort())) {
                                interceptingNextClientStrategy.delegate.favor(connectionDescriptor);
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            if (response != null && response.solutions != null && !response.solutions.isEmpty()) {
                MiruSolution solution = response.solutions.get(0);
                MiruHost host = solution.usedPartition.host;
                strategyCache.compute(tenantId, (key, existing) -> {
                    if (existing != null && existing instanceof PreferredNodeStrategy && ((PreferredNodeStrategy) existing).host.equals(host)) {
                        return existing;
                    } else {
                        return new PreferredNodeStrategy(host);
                    }
                });
            }
        }
    }


    private static final class PreferredNodeStrategy implements NextClientStrategy, IndexedClientStrategy {

        private final ReturnFirstNonFailure returnFirstNonFailure = new ReturnFirstNonFailure();
        private final MiruHost host;

        PreferredNodeStrategy(MiruHost host) {
            this.host = host;
        }

        @Override
        public <C, R> R call(String family,
            ClientCall<C, R, HttpClientException> httpCall,
            ConnectionDescriptor[] connectionDescriptors,
            long connectionDescriptorsVersion,
            C[] clients,
            ClientHealth[] clientHealths,
            int deadAfterNErrors,
            long checkDeadEveryNMillis,
            AtomicInteger[] clientsErrors,
            AtomicLong[] clientsDeathTimestamp) throws HttpClientException {
            return returnFirstNonFailure.call(this,
                family,
                httpCall,
                connectionDescriptors,
                connectionDescriptorsVersion,
                clients,
                clientHealths,
                deadAfterNErrors,
                checkDeadEveryNMillis,
                clientsErrors,
                clientsDeathTimestamp);
        }

        @Override
        public int[] getClients(ConnectionDescriptor[] connectionDescriptors) {
            int len = connectionDescriptors.length;
            int[] indexes = new int[len];
            int pos = 0;
            int preferredIndex = -1;
            for (int i = 0; i < connectionDescriptors.length; i++) {
                HostPort hostPort = connectionDescriptors[i].getHostPort();
                InstanceDescriptor instanceDescriptor = connectionDescriptors[i].getInstanceDescriptor();
                if (MiruHostProvider.checkEquals(host,
                    instanceDescriptor.instanceName, instanceDescriptor.instanceKey,
                    hostPort.getHost(), hostPort.getPort())) {
                    indexes[0] = i;
                    pos = 1;
                    preferredIndex = i;
                    break;
                }
            }
            for (int i = 0; i < connectionDescriptors.length; i++) {
                if (i != preferredIndex) {
                    indexes[pos] = i;
                    pos++;
                }
            }
            return indexes;
        }

        @Override
        public void usedClientAtIndex(int i) {
        }
    }


}
