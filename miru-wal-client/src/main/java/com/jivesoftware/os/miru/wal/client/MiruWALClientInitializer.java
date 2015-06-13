package com.jivesoftware.os.miru.wal.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;

public class MiruWALClientInitializer {

    public <C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> MiruWALClient<C, S> initialize(String routingTenantId,
        TenantAwareHttpClient<String> client,
        ObjectMapper mapper,
        long sleepOnFailureMillis,
        String pathPrefix,
        Class<C> cursorClass,
        Class<S> sipCursorClass) throws Exception {

        return new MiruHttpWALClient<>(routingTenantId, client, new RoundRobinStrategy(), mapper, new HttpResponseMapper(mapper), sleepOnFailureMillis,
            pathPrefix, cursorClass, sipCursorClass);
    }
}
