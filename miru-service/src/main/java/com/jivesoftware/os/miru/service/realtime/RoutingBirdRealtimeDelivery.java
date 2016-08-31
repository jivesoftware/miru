package com.jivesoftware.os.miru.service.realtime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.realtime.MiruRealtimeDelivery;
import com.jivesoftware.os.miru.api.realtime.RealtimeUpdate;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall.ClientResponse;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import java.util.List;

/**
 *
 */
public class RoutingBirdRealtimeDelivery implements MiruRealtimeDelivery {

    private final TenantAwareHttpClient<String> deliveryClient;
    private final NextClientStrategy nextClientStrategy;
    private final String deliveryEndpoint;
    private final ObjectMapper objectMapper;
    private final MiruStats miruStats;

    public RoutingBirdRealtimeDelivery(TenantAwareHttpClient<String> deliveryClient,
        NextClientStrategy nextClientStrategy,
        String deliveryEndpoint,
        ObjectMapper objectMapper, MiruStats miruStats) {
        this.deliveryClient = deliveryClient;
        this.nextClientStrategy = nextClientStrategy;
        this.deliveryEndpoint = deliveryEndpoint;
        this.objectMapper = objectMapper;
        this.miruStats = miruStats;
    }

    @Override
    public void deliver(MiruPartitionCoord coord, List<Long> activityTimes) throws Exception {
        long start = System.currentTimeMillis();
        try {
            deliveryClient.call("", nextClientStrategy, "deliverRealtime", httpClient -> {
                String json = null;
                try {
                    json = objectMapper.writeValueAsString(new RealtimeUpdate(coord, activityTimes));
                } catch (JsonProcessingException e) {
                    throw new MiruRealtimeDeliveryException("Failed to serialize update", e);
                }
                HttpResponse httpResponse = httpClient.postJson(deliveryEndpoint, json, null);
                if (httpResponse.getStatusCode() < 200 || httpResponse.getStatusCode() >= 300) {
                    throw new MiruRealtimeDeliveryException("Invalid response code: " + httpResponse.getStatusCode());
                }
                return new ClientResponse<Void>(null, true);
            });
            miruStats.egressed("realtime>delivery>success", activityTimes.size(), System.currentTimeMillis() - start);
        } catch (Exception e) {
            miruStats.egressed("realtime>delivery>failure", activityTimes.size(), System.currentTimeMillis() - start);
            throw e;
        }
    }
}
