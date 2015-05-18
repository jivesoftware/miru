package com.jivesoftware.os.miru.wal.deployable;

import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;
import org.merlin.config.Config;
import org.merlin.config.defaults.ClassDefault;

public interface RCVSWALConfig extends Config {

    @ClassDefault(IllegalStateException.class)
    <C extends Config, I extends RowColumnValueStoreInitializer<E>, E extends Exception> Class<RowColumnValueStoreProvider<C, I, E>>
    getRowColumnValueStoreProviderClass();
}
