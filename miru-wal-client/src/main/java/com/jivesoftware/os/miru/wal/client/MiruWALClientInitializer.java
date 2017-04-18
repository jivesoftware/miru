package com.jivesoftware.os.miru.wal.client;

import com.jivesoftware.os.routing.bird.health.api.SickHealthCheckConfig;
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

}
