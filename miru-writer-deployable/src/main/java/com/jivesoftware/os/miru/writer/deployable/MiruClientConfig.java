package com.jivesoftware.os.miru.writer.deployable;

import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.ClassDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;

public interface MiruClientConfig extends Config {

    @IntDefault(5_000_000)
    Integer getTotalCapacity();

    @IntDefault(24)
    Integer getSendActivitiesThreadPoolSize();

    @IntDefault(10_000)
    Integer getSocketTimeoutInMillis();

    @IntDefault(100)
    Integer getMaxConnections();

    @IntDefault(3_000)
    Integer getTopologyCacheSize();

    @LongDefault(1_000 * 60 * 60)
    Long getTopologyCacheExpiresInMillis();

    // 1 week
    @LongDefault(1_000 * 60 * 60 * 24 * 7)
    Long getPartitionMaximumAgeInMillis();

    @BooleanDefault(true)
    Boolean getLiveIngress();

    @ClassDefault(IllegalStateException.class)
    <C extends Config, I extends RowColumnValueStoreInitializer<E>, E extends Exception> Class<RowColumnValueStoreProvider<C, I, E>>
    getRowColumnValueStoreProviderClass();
}
