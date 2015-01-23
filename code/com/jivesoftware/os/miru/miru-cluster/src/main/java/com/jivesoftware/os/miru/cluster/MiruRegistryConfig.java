package com.jivesoftware.os.miru.cluster;

import com.jivesoftware.os.miru.cluster.rcvs.ThrowsExceptionRowColumnValueStoreProvider;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;
import org.merlin.config.Config;
import org.merlin.config.defaults.ClassDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;

public interface MiruRegistryConfig extends Config {

    @IntDefault(3)
    int getDefaultNumberOfReplicas();

    // 1 hour
    @LongDefault(3_600_000)
    long getDefaultTopologyIsStaleAfterMillis();

    @ClassDefault(ThrowsExceptionRowColumnValueStoreProvider.class)
    <C extends Config, I extends RowColumnValueStoreInitializer<E>, E extends Exception> Class<RowColumnValueStoreProvider<C, I, E>>
    getRowColumnValueStoreProviderClass();
}
