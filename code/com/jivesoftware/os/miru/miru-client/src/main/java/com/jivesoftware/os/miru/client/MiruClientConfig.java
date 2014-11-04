package com.jivesoftware.os.miru.client;

import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public interface MiruClientConfig extends Config {

    @IntDefault(5_000_000)
    Integer getTotalCapacity();

    @IntDefault(24)
    Integer getSendActivitiesThreadPoolSize();

    @IntDefault(10_000)
    Integer getSocketTimeoutInMillis();

    @IntDefault(100)
    Integer getMaxConnections();

    // This is a default/override used only by naive in-process implementations. Real implementations should read from an hbase registry.
    @StringDefault("")
    String getDefaultHostAddresses();

    @IntDefault(3_000)
    Integer getTopologyCacheSize();

    @LongDefault(1_000 * 60 * 60)
    Long getTopologyCacheExpiresInMillis();

    @LongDefault(1_000 * 60 * 60 * 24 * 7) // 1 week
    Long getPartitionMaximumAgeInMillis();
}
