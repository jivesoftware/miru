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

    // 4 KB
    @LongDefault(4_096)
    long getOnDiskInitialChunkSize();

    // 32 MB
    @LongDefault(33_554_432)
    long getInMemoryChunkSize();

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
    long getPartitionRebuildIntervalInMillis();

    @LongDefault(5_000)
    long getPartitionSipMigrateIntervalInMillis();

    @LongDefault(30_000)
    long getPartitionBanUnregisteredSchemaMillis();

    @LongDefault(600_000)
    long getPartitionReleaseContextCacheAfterMillis();

    @LongDefault(3_000)
    long getPartitionMigrationWaitInMillis();

    @IntDefault(3)
    int getPartitionNumberOfChunkStores();

    void setPartitionNumberOfChunkStores(int partitionNumberOfChunkStores);

    @IntDefault(10_000)
    int getPartitionAuthzCacheSize();

    @BooleanDefault(true)
    boolean getPartitionDeleteChunkStoreOnClose();

    @IntDefault(65_536)
    int getFieldIndexNumberOfLocks();

    @IntDefault(65_536)
    int getStreamNumberOfLocks();

    @IntDefault(1_024)
    int getAuthzNumberOfLocks();

    @IntDefault(65_536)
    int getChunkStoreNumberOfLocks();

    @IntDefault(65_536)
    int getKeyedFilerNumberOfLocks();

    @StringDefault("memory")
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

    void setRebuildIndexerThreads(int rebuildIndexerThreads);

    @IntDefault(24)
    int getSipIndexerThreads();

    @BooleanDefault(true)
    public boolean getUseOffHeapBuffers();

    @LongDefault(10_000_000)
    long getMaxRebuildActivityCount();
}
