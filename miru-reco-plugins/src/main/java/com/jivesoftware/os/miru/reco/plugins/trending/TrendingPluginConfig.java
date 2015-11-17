package com.jivesoftware.os.miru.reco.plugins.trending;

import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;

/**
 *
 */
public interface TrendingPluginConfig extends Config {

    @IntDefault(1_000_000)
    int getQueryCacheMaxSize();
}
