package com.jivesoftware.os.miru.service;

import org.merlin.config.Config;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public interface MiruServiceConfig extends Config {

    @IntDefault(8_192)
    int getBitsetBufferSize();

    @IntDefault(10)
    int getStreamFactoryExecutorCount();

    @StringDefault("com.jivesoftware")
    String getPluginPackages();

    @StringDefault("var/lib/miru/data")
    String getDiskResourceLocatorPath();

    @StringDefault("var/lib/miru/transient")
    String getTransientResourceLocatorPath();

    @LongDefault(33_554_432) // 32 MB
    long getDiskResourceInitialChunkSize();

    @LongDefault(4_096) // 4 KB
    long getTransientResourceInitialChunkSize();

    @LongDefault(5_000)
    long getHeartbeatIntervalInMillis();

    @LongDefault(5_000)
    long getEnsurePartitionsIntervalInMillis();

    @IntDefault(1)
    int getDefaultInitialSolvers();

    @IntDefault(10)
    int getDefaultMaxNumberOfSolvers();

    @LongDefault(100)
    long getDefaultAddAnotherSolverAfterNMillis();

    @LongDefault(3_000)
    long getDefaultFailAfterNMillis();
    void setDefaultFailAfterNMillis(long millis);

    @IntDefault(1_000)
    int getPartitionRebuildBatchSize();

    @LongDefault(5_000)
    long getPartitionBootstrapIntervalInMillis();

    @LongDefault(5_000)
    long getPartitionRunnableIntervalInMillis();

    @IntDefault(10_000)
    int getPartitionAuthzCacheSize();

    @StringDefault("hybrid")
    String getDefaultStorage();
    void setDefaultStorage(String storageType);

    @IntDefault(1_000)
    int getLongTailSolverWindowSize();

    @IntDefault(95)
    int getLongTailSolverPercentile();

    @StringDefault("")
    String getReadStreamIdsPropName();
}
