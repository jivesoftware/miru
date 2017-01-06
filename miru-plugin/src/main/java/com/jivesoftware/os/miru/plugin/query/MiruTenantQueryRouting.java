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
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptor;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import com.jivesoftware.os.routing.bird.shared.InstanceDescriptor;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import java.util.Map;

/**
 *
 */
public class MiruTenantQueryRouting {

    private final RoundRobinStrategy robinStrategy = new RoundRobinStrategy();
    private final Map<MiruTenantId, NextClientStrategy> strategyCache = Maps.newConcurrentMap();

    public <Q, A, T> MiruResponse<A> query(T routingTenant,
        String family,
        TenantAwareHttpClient<T> reader,
        ObjectMapper requestMapper,
        HttpResponseMapper responseMapper,
        MiruRequest<Q> request,
        String path,
        Class<A> answerClass) throws Exception {

        String json = requestMapper.writeValueAsString(request);
        HttpResponse httpResponse = reader.call(routingTenant, getTenantStrategy(request.tenantId), family,
            (c) -> new ClientCall.ClientResponse<>(
                c.postJson(path, json, null), true));

        @SuppressWarnings("unchecked")
        MiruResponse<A> response = responseMapper.extractResultFromResponse(httpResponse,
            MiruResponse.class, new Class[] { answerClass }, null);
        recordTenantStrategy(request.tenantId, response);

        return response;
    }

    private NextClientStrategy getTenantStrategy(MiruTenantId tenantId) {
        return strategyCache.getOrDefault(tenantId, robinStrategy);
    }

    private void recordTenantStrategy(MiruTenantId tenantId, MiruResponse<?> response) {
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

    private static final class PreferredNodeStrategy implements NextClientStrategy {

        private final MiruHost host;

        public PreferredNodeStrategy(MiruHost host) {
            this.host = host;
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
