package com.jivesoftware.os.miru.stream.plugins.strut;

import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;

/**
 *
 * @author jonathan.colt
 */
public interface StrutConfig extends Config {

    @LongDefault(60 * 60 * 1_000L)
    long getModelCacheExpirationInMillis();

    @LongDefault(1024)
    long getModelCacheMaxSize();

    @IntDefault(24)
    int getAsyncThreadPoolSize();

    @IntDefault(1_000)
    int getMaxUpdatesBeforeFlush();
}
