package com.jivesoftware.os.miru.stream.plugins.fulltext;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.ClassDefault;
import org.merlin.config.defaults.IntDefault;

/**
 * @author jonathan.colt
 */
public interface FullTextConfig extends Config {

    @BooleanDefault(true)
    boolean getGathererEnabled();

    @IntDefault(4)
    int getGathererThreadPoolSize();

    @IntDefault(10_000)
    int getGathererBatchSize();

    @ClassDefault(DisabledTermProviderInitializer.class)
    Class<? extends FullTextTermProviderInitializer> getTermProviderInitializerClass();

}
