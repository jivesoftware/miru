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

    @StringDefault("hbase")
    String getClusterRegistryType(); // hbase or amza

    // 1 hour
    @LongDefault(3_600_000)
    long getDefaultTopologyIsStaleAfterMillis();

    // 1 hour
    @LongDefault(600_000)
    long getDefaultTopologyIsIdleAfterMillis();

    @ClassDefault(IllegalStateException.class)
    <C extends Config, I extends RowColumnValueStoreInitializer<E>, E extends Exception> Class<RowColumnValueStoreProvider<C, I, E>>
    getRowColumnValueStoreProviderClass();
}
