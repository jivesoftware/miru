package com.jivesoftware.os.miru.cluster.rcvs;

import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;
import org.merlin.config.Config;

/**
 *
 */
public class ThrowsExceptionRowColumnValueStoreProvider implements RowColumnValueStoreProvider<Config, RowColumnValueStoreInitializer<Exception>, Exception> {

    @Override
    public Class<Config> getConfigurationClass() {
        throw new UnsupportedOperationException("RowColumnValueStoreProvider is not configured");
    }

    @Override
    public RowColumnValueStoreInitializer<Exception> create(Config config) {
        throw new UnsupportedOperationException("RowColumnValueStoreProvider is not configured");
    }
}
