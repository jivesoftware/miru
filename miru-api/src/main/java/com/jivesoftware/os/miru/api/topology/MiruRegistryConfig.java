package com.jivesoftware.os.miru.api.topology;

import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;
import org.merlin.config.Config;
import org.merlin.config.defaults.ClassDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;


public interface MiruRegistryConfig extends Config {

    @IntDefault(3)
    int getDefaultNumberOfReplicas();

    @StringDefault("rcvs")
    String getClusterRegistryType(); // rcvs or amza

    // 1 hour
    @LongDefault(3_600_000)
    long getDefaultTopologyIsStaleAfterMillis();

    // 10 minutes
    @LongDefault(600_000)
    long getDefaultTopologyIsIdleAfterMillis();

    // 90 days
    @LongDefault(7_776_000_000L)
    long getDefaultTopologyDestroyAfterMillis();

    @ClassDefault(IllegalStateException.class)
    <C extends Config, I extends RowColumnValueStoreInitializer<E>, E extends Exception> Class<RowColumnValueStoreProvider<C, I, E>>
    getRowColumnValueStoreProviderClass();
}
