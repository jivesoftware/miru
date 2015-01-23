package com.jivesoftware.os.miru.cluster.rcvs;

import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;
import org.merlin.config.Config;

/**
 *
 */
public class ThrowsExceptionRowColumnValueStoreProvider implements RowColumnValueStoreProvider<Config, Exception> {

    @Override
    public Class<Config> getConfigurationClass() {
        throw new UnsupportedOperationException("RowColumnValueStoreProvider is not configured");
    }

    @Override
    public Class<? extends RowColumnValueStoreInitializer<Exception>> getInitializerClass() {
        throw new UnsupportedOperationException("RowColumnValueStoreProvider is not configured");
    }
}
