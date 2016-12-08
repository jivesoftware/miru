package com.jivesoftware.os.miru.stream.plugins.fulltext;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.ClassDefault;
import org.merlin.config.defaults.IntDefault;

/**
 * @author jonathan.colt
 */
public interface FullTextConfig extends Config {

    @ClassDefault(DisabledTermProvider.class)
    Class<? extends FullTextTermProvider> getTermProviderClass();

    @IntDefault(4)
    int getAsyncThreadPoolSize();

    @IntDefault(10_000)
    int getUpdateBatchSize();

    @BooleanDefault(true)
    boolean getRemoteSnappyCompression();
}
