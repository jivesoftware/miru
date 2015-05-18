package com.jivesoftware.os.miru.wal.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.http.client.rest.ResponseMapper;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantAwareHttpClient;

public class MiruWALClientInitializer {

    public <C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> MiruWALClient<C, S> initialize(String routingTenantId,
        TenantAwareHttpClient<String> client,
        ObjectMapper mapper,
        long sleepOnFailureMillis,
        String pathPrefix,
        Class<C> cursorClass,
        Class<S> sipCursorClass) throws Exception {

        return new MiruHttpWALClient<>(routingTenantId, client, mapper, new ResponseMapper(mapper), sleepOnFailureMillis,
            pathPrefix, cursorClass, sipCursorClass);
    }
}
