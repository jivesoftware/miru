package com.jivesoftware.os.miru.service.partition;

import com.google.common.hash.Hashing;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.BloomIndex;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.service.stream.MiruIndexAuthz;
import com.jivesoftware.os.miru.service.stream.MiruIndexBloom;
import com.jivesoftware.os.miru.service.stream.MiruIndexFieldValues;
import com.jivesoftware.os.miru.service.stream.MiruIndexLatest;
import com.jivesoftware.os.miru.service.stream.MiruIndexPairedLatest;
import com.jivesoftware.os.miru.service.stream.MiruIndexer;
import com.jivesoftware.os.miru.service.stream.MiruRebuildDirector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author jonathan
 */
public class MiruLocalPartitionFactory {

    private final MiruStats miruStats;
    private final MiruServiceConfig config;
    private final MiruContextFactory miruContextFactory;
    private final MiruWALClient walClient;
    private final MiruPartitionHeartbeatHandler partitionEventHandler;
    private final MiruRebuildDirector rebuildDirector;
    private final ScheduledExecutorService scheduledBoostrapExecutor;
    private final ScheduledExecutorService scheduledRebuildExecutor;
    private final ScheduledExecutorService scheduledSipMigrateExecutor;
    private final ExecutorService rebuildExecutors;
    private final ExecutorService sipIndexExecutor;
    private final ExecutorService mergeExecutor;
    private final int rebuildIndexerThreads;
    private final MiruIndexRepairs indexRepairs;
    private final MiruMergeChits mergeChits;

    public MiruLocalPartitionFactory(MiruStats miruStats,
        MiruServiceConfig config,
        MiruContextFactory miruContextFactory,
        MiruWALClient walClient,
        MiruPartitionHeartbeatHandler partitionEventHandler,
        MiruRebuildDirector rebuildDirector,
        ScheduledExecutorService scheduledBoostrapExecutor,
        ScheduledExecutorService scheduledRebuildExecutor,
        ScheduledExecutorService scheduledSipMigrateExecutor,
        ExecutorService rebuildExecutors,
        ExecutorService sipIndexExecutor,
        ExecutorService mergeExecutor,
        int rebuildIndexerThreads,
        MiruIndexRepairs indexRepairs,
        MiruMergeChits mergeChits) {

        this.miruStats = miruStats;
        this.config = config;
        this.miruContextFactory = miruContextFactory;
        this.walClient = walClient;
        this.partitionEventHandler = partitionEventHandler;
        this.rebuildDirector = rebuildDirector;
        this.scheduledBoostrapExecutor = scheduledBoostrapExecutor;
        this.scheduledRebuildExecutor = scheduledRebuildExecutor;
        this.scheduledSipMigrateExecutor = scheduledSipMigrateExecutor;
        this.rebuildExecutors = rebuildExecutors;
        this.sipIndexExecutor = sipIndexExecutor;
        this.mergeExecutor = mergeExecutor;
        this.rebuildIndexerThreads = rebuildIndexerThreads;
        this.indexRepairs = indexRepairs;
        this.mergeChits = mergeChits;
    }

    public <BM> MiruLocalHostedPartition<BM> create(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, long expireAfterMillis) throws Exception {
        return new MiruLocalHostedPartition<>(miruStats,
            bitmaps,
            coord,
            expireAfterMillis > 0 ? System.currentTimeMillis() + expireAfterMillis : -1,
            miruContextFactory,
            walClient,
            partitionEventHandler,
            rebuildDirector,
            scheduledBoostrapExecutor,
            scheduledRebuildExecutor,
            scheduledSipMigrateExecutor,
            rebuildExecutors,
            sipIndexExecutor,
            mergeExecutor,
            rebuildIndexerThreads,
            indexRepairs,
            new MiruIndexer<>(
                new MiruIndexAuthz<BM>(),
                new MiruIndexFieldValues<BM>(),
                new MiruIndexBloom<>(new BloomIndex<>(bitmaps, Hashing.murmur3_128(), 100_000, 0.01f)),
                new MiruIndexLatest<BM>(),
                new MiruIndexPairedLatest<BM>()),
            config.getPartitionWakeOnIndex(),
            config.getPartitionRebuildIfBehindByCount(),
            config.getPartitionRebuildBatchSize(),
            config.getPartitionSipBatchSize(),
            mergeChits,
            new MiruLocalHostedPartition.Timings(
                config.getPartitionBootstrapIntervalInMillis(),
                config.getPartitionRebuildIntervalInMillis(),
                config.getPartitionSipMigrateIntervalInMillis(),
                config.getPartitionBanUnregisteredSchemaMillis(),
                config.getPartitionMigrationWaitInMillis()));
    }

    public void prioritizeRebuild(MiruLocalHostedPartition<?> partition) {
        rebuildDirector.prioritize(partition);
    }
}
