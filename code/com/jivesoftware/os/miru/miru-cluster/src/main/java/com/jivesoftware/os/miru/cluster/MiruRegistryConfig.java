package com.jivesoftware.os.miru.cluster;

import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;

public interface MiruRegistryConfig extends Config {

    @IntDefault(3)
    int getDefaultNumberOfReplicas();

    @LongDefault(3_600_000) // 1 hour
    long getDefaultTopologyIsStaleAfterMillis();
}
