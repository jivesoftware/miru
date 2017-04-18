package com.jivesoftware.os.miru.wal.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreads;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import java.util.concurrent.ExecutorService;

public class AmzaWALClientInitializer {

    public AmzaHttpWALClient initialize(String routingTenantId,
        TenantAwareHttpClient<String> client,
        ExecutorService tasExecutors,
        int tasWindowSize,
        float tasPercentile,
        long tasInitialSLAMillis,
        ObjectMapper mapper,
        SickThreads sickThreads,
        long sleepOnFailureMillis) throws Exception {

        return new AmzaHttpWALClient(routingTenantId,
            client,
            tasExecutors,
            tasWindowSize,
            tasPercentile,
            tasInitialSLAMillis,
            mapper,
            new HttpResponseMapper(mapper),
            sickThreads,
            sleepOnFailureMillis);
    }
}
