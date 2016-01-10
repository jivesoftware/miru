package com.jivesoftware.os.miru.wal.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.routing.bird.health.api.SickHealthCheckConfig;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreads;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import org.merlin.config.defaults.DoubleDefault;
import org.merlin.config.defaults.StringDefault;

public class MiruWALClientInitializer {

    public interface WALClientSickThreadsHealthCheckConfig extends SickHealthCheckConfig {

        @Override
        @StringDefault("sick>threads>walClient")
        String getName();

        @Override
        @StringDefault("No WAL client threads are spinning")
        String getDescription();

        @DoubleDefault(0.2)
        Double getSickHealth();
    }

    public <C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> MiruWALClient<C, S> initialize(String routingTenantId,
        TenantAwareHttpClient<String> client,
        ObjectMapper mapper,
        SickThreads sickThreads,
        long sleepOnFailureMillis,
        String pathPrefix,
        Class<C> cursorClass,
        Class<S> sipCursorClass) throws Exception {

        return new MiruHttpWALClient<>(routingTenantId, client, new RoundRobinStrategy(), mapper, new HttpResponseMapper(mapper), sickThreads,
            sleepOnFailureMillis,
            pathPrefix, cursorClass, sipCursorClass);
    }
}
