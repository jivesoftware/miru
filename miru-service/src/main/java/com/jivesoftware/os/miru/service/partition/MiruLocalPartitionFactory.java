package com.jivesoftware.os.miru.service.partition;

import com.google.common.hash.Hashing;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
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
public class MiruLocalPartitionFactory<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> {

    private final MiruStats miruStats;
    private final MiruServiceConfig config;
    private final MiruContextFactory<S> miruContextFactory;
    private final MiruSipTrackerFactory<S> sipTrackerFactory;
    private final MiruWALClient<C, S> walClient;
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
        MiruContextFactory<S> miruContextFactory,
        MiruSipTrackerFactory<S> sipTrackerFactory,
        MiruWALClient<C, S> walClient,
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
        this.sipTrackerFactory = sipTrackerFactory;
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

    public <BM extends IBM, IBM> MiruLocalHostedPartition<BM, IBM, C, S> create(MiruBitmaps<BM, IBM> bitmaps,
        MiruPartitionCoord coord,
        long expireAfterMillis) throws Exception {

        return new MiruLocalHostedPartition<>(miruStats,
            bitmaps,
            coord,
            expireAfterMillis > 0 ? System.currentTimeMillis() + expireAfterMillis : -1,
            miruContextFactory,
            sipTrackerFactory,
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
                new MiruIndexAuthz<>(),
                new MiruIndexFieldValues<>(),
                new MiruIndexBloom<>(new BloomIndex<>(bitmaps, Hashing.murmur3_128(), 100_000, 0.01f)),
                new MiruIndexLatest<>(),
                new MiruIndexPairedLatest<>()),
            config.getPartitionRebuildIfBehindByCount(),
            config.getPartitionRebuildBatchSize(),
            config.getPartitionSipBatchSize(),
            mergeChits,
            new MiruLocalHostedPartition.Timings(
                config.getPartitionBootstrapIntervalInMillis(),
                config.getPartitionRebuildIntervalInMillis(),
                config.getPartitionSipMigrateIntervalInMillis(),
                config.getPartitionBanUnregisteredSchemaMillis(),
                config.getPartitionMigrationWaitInMillis(),
                config.getPartitionSipNotifyEndOfStreamMillis()));
    }

    public void prioritizeRebuild(MiruLocalHostedPartition<?, ?, ?, ?> partition) {
        rebuildDirector.prioritize(partition);
    }
}
