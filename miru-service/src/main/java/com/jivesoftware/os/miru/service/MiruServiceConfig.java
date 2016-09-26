package com.jivesoftware.os.miru.service;

import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.ClassDefault;
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

    @IntDefault(10_000)
    int getPartitionRebuildBatchSize();

    @IntDefault(10_000)
    int getPartitionSipBatchSize();

    @LongDefault(1_000_000)
    long getPersistentMergeChitCount();

    void setPersistentMergeChitCount(long mergeChitCount);

    @LongDefault(1_000_000)
    long getTransientMergeChitCount();

    void setTransientMergeChitCount(long mergeChitCount);

    @LongDefault(-1)
    long getMergeMaxOverage();

    @BooleanDefault(true)
    boolean getPartitionAllowNonLatestSchemaInteractions();

    @LongDefault(5_000)
    long getPartitionBootstrapIntervalInMillis();

    @LongDefault(5_000)
    long getPartitionRebuildIntervalInMillis();

    @LongDefault(5_000)
    long getPartitionSipMigrateIntervalInMillis();

    @LongDefault(30_000)
    long getPartitionBanUnregisteredSchemaMillis();

    @LongDefault(3_000)
    long getPartitionMigrationWaitInMillis();

    @LongDefault(30_000)
    long getPartitionSipNotifyEndOfStreamMillis();

    @LongDefault(300_000)
    long getPartitionRebuildEstimateActivityCountIntervalInMillis();

    @IntDefault(3)
    int getPartitionNumberOfChunkStores();

    void setPartitionNumberOfChunkStores(int partitionNumberOfChunkStores);

    @IntDefault(10_000)
    int getPartitionAuthzCacheSize();

    @BooleanDefault(true)
    boolean getPartitionDeleteChunkStoreOnClose();

    @IntDefault(10_000)
    int getPartitionInitialChunkCacheSize();

    @IntDefault(100_000)
    int getPartitionMaxChunkCacheSize();

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

    @IntDefault(8)
    int getParallelSolversExecutorThreads();

    @IntDefault(24)
    int getRebuilderThreads();

    @IntDefault(24)
    int getRebuildIndexerThreads();

    void setRebuildIndexerThreads(int rebuildIndexerThreads);

    @IntDefault(24)
    int getSipIndexerThreads();

    @IntDefault(8)
    int getMergeIndexThreads();

    @BooleanDefault(true)
    boolean getUseOffHeapBuffers();

    @LongDefault(10_000_000)
    long getMaxRebuildActivityCount();

    @ClassDefault(IllegalStateException.class)
    Class<? extends MiruBitmaps<?, ?>> getBitmapsClass();

    @BooleanDefault(true)
    boolean getEnableTermInterning();

    @BooleanDefault(true)
    boolean getUseLabIndexes();

    @LongDefault(1024 * 1024 * 1024)
    long getLabMaxHeapPressureInBytes();

    @LongDefault(100_000)
    long getLabLeapCacheMaxCapacity();

    @IntDefault(32)
    int getLabLeapCacheConcurrency();

    @LongDefault(60_000)
    long getLabLeapCacheCleanupIntervalMillis();

    @BooleanDefault(true)
    boolean getFsyncOnCommit();

    @LongDefault(1024 * 1024 * 1024)
    long getGlobalLabMaxHeapPressureInBytes();

    @LongDefault(2L * 1024 * 1024 * 1024)
    long getGlobalLabBlockOnHeapPressureInBytes();

    @IntDefault(3)
    int getGlobalLabHeapPressureStripes();

    @LongDefault(2L * 1024 * 1024 * 1024)
    long getRebuildLabMaxHeapPressureInBytes();

    @LongDefault(4L * 1024 * 1024 * 1024)
    long getRebuildLabBlockOnHeapPressureInBytes();

    @StringDefault("mostBytesFirst")
    String getLabFreeHeapStrategy();
    
    @IntDefault(3)
    int getRebuildLabHeapPressureStripes();

    @LongDefault(1024 * 1024 * 1024)
    long getLabMaxWALSizeInBytes();

    @LongDefault(1_000_000)
    long getLabMaxEntriesPerWAL();

    @LongDefault(100 * 1024 * 1024)
    long getLabMaxEntrySizeInBytes();

    @LongDefault(1024 * 1024 * 1024)
    long getLabMaxWALOnOpenHeapPressureOverride();

    @BooleanDefault(false)
    boolean getLabUseOffHeap();

    @IntDefault(4)
    int getTimeIdKeepNIndexes();

    @IntDefault(100_000_000)
    int getTimeIdMaxEntriesPerIndex();

    @LongDefault(10L * 1024 * 1024)
    long getTimeIdMaxHeapPressureInBytes();

    @LongDefault(100L * 1024 * 1024)
    long getTimeIdLabMaxHeapPressureInBytes();

    @LongDefault(200L * 1024 * 1024)
    long getTimeIdLabBlockOnHeapPressureInBytes();

    @BooleanDefault(true)
    boolean getTimeIdFsyncOnAppend();

    @StringDefault("")
    String getRealtimeDeliveryService();

    @StringDefault("")
    String getRealtimeDeliveryEndpoint();

    @BooleanDefault(true)
    boolean getRealtimeDelivery();
}
