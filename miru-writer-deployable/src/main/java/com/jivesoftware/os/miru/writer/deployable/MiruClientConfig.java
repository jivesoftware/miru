package com.jivesoftware.os.miru.writer.deployable;

import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;

public interface MiruClientConfig extends Config {

    @IntDefault(1_000_000)
    Integer getTotalCapacity();

    @IntDefault(24)
    Integer getSendActivitiesThreadPoolSize();

    // 1 week
    @LongDefault(1_000 * 60 * 60 * 24 * 7)
    Long getPartitionMaximumAgeInMillis();

}
