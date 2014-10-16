package com.jivesoftware.os.miru.service;

import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
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
    String getDiskResourceLocatorPaths();

    @StringDefault("var/lib/miru/transient")
    String getTransientResourceLocatorPaths();

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

    @LongDefault(30_000)
    long getDefaultFailAfterNMillis();
    void setDefaultFailAfterNMillis(long millis);

    @BooleanDefault(true)
    boolean getPartitionWakeOnIndex();

    @IntDefault(10_000)
    int getPartitionRebuildBatchSize();

    @IntDefault(1_000)
    int getPartitionSipBatchSize();

    @LongDefault(10_000)
    long getPartitionRebuildFailureSleepMillis();

    @LongDefault(5_000)
    long getPartitionBootstrapIntervalInMillis();

    @LongDefault(5_000)
    long getPartitionRunnableIntervalInMillis();

    @LongDefault(30_000)
    long getPartitionBanUnregisteredSchemaMillis();

    @IntDefault(10_000)
    int getPartitionAuthzCacheSize();

    @IntDefault(1_000)
    int getHybridFieldInitialPageCapacity();

    @StringDefault("hybrid")
    String getDefaultStorage();
    void setDefaultStorage(String storageType);

    @IntDefault(1_000)
    int getLongTailSolverWindowSize();

    @IntDefault(95)
    int getLongTailSolverPercentile();

    @StringDefault("")
    String getReadStreamIdsPropName();

    @IntDefault(24)
    int getPartitionScheduledBootstrapThreads();

    @IntDefault(24)
    int getPartitionScheduledRebuildThreads();

    @IntDefault(24)
    int getPartitionScheduledSipMigrateThreads();

    @IntDefault(24)
    int getSolverExecutorThreads();

    @IntDefault(24)
    int getRebuilderThreads();

    @IntDefault(24)
    int getRebuildIndexerThreads();

    @IntDefault(24)
    int getSipIndexerThreads();
    
    @BooleanDefault(true)
    public boolean getUseOffHeapBuffers();
    
    @BooleanDefault(false)
    public boolean getTransientIsFileBackedChunkStore();
}
