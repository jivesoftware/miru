package com.jivesoftware.os.miru.api.topology;

import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;


public interface MiruRegistryConfig extends Config {

    @IntDefault(3)
    int getDefaultNumberOfReplicas();

    // 1 hour
    @LongDefault(3_600_000)
    long getDefaultTopologyIsStaleAfterMillis();

    // 10 minutes
    @LongDefault(600_000)
    long getDefaultTopologyIsIdleAfterMillis();

    // 90 days
    @LongDefault(7_776_000_000L)
    long getDefaultTopologyDestroyAfterMillis();

    // 180 days
    @LongDefault(15_552_000_000L)
    long getDefaultTopologyCleanupAfterMillis();
}
