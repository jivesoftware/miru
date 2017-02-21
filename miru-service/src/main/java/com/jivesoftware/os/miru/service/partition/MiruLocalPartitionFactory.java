package com.jivesoftware.os.miru.service.partition;

import com.google.common.hash.Hashing;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.realtime.MiruRealtimeDelivery;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.BloomIndex;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.service.stream.MiruIndexAuthz;
import com.jivesoftware.os.miru.service.stream.MiruIndexBloom;
import com.jivesoftware.os.miru.service.stream.MiruIndexCallbacks;
import com.jivesoftware.os.miru.service.stream.MiruIndexLatest;
import com.jivesoftware.os.miru.service.stream.MiruIndexPairedLatest;
import com.jivesoftware.os.miru.service.stream.MiruIndexPrimaryFields;
import com.jivesoftware.os.miru.service.stream.MiruIndexValueBits;
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
    private final MiruRealtimeDelivery realtimeDelivery;
    private final MiruPartitionHeartbeatHandler partitionEventHandler;
    private final MiruRebuildDirector rebuildDirector;
    private final ScheduledExecutorService scheduledBoostrapExecutor;
    private final ScheduledExecutorService scheduledRebuildExecutor;
    private final ScheduledExecutorService scheduledSipMigrateExecutor;
    private final ExecutorService rebuildExecutors;
    private final ExecutorService sipIndexExecutor;
    private final ExecutorService persistentMergeExecutor;
    private final ExecutorService transientMergeExecutor;
    private final int rebuildIndexerThreads;
    private final MiruIndexCallbacks indexCallbacks;
    private final MiruIndexRepairs indexRepairs;
    private final MiruMergeChits persistentMergeChits;
    private final MiruMergeChits transientMergeChits;
    private final PartitionErrorTracker partitionErrorTracker;

    public MiruLocalPartitionFactory(MiruStats miruStats,
        MiruServiceConfig config,
        MiruContextFactory<S> miruContextFactory,
        MiruSipTrackerFactory<S> sipTrackerFactory,
        MiruWALClient<C, S> walClient,
        MiruRealtimeDelivery realtimeDelivery,
        MiruPartitionHeartbeatHandler partitionEventHandler,
        MiruRebuildDirector rebuildDirector,
        ScheduledExecutorService scheduledBoostrapExecutor,
        ScheduledExecutorService scheduledRebuildExecutor,
        ScheduledExecutorService scheduledSipMigrateExecutor,
        ExecutorService rebuildExecutors,
        ExecutorService sipIndexExecutor,
        ExecutorService persistentMergeExecutor,
        ExecutorService transientMergeExecutor,
        int rebuildIndexerThreads,
        MiruIndexCallbacks indexCallbacks,
        MiruIndexRepairs indexRepairs,
        MiruMergeChits persistentMergeChits,
        MiruMergeChits transientMergeChits,
        PartitionErrorTracker partitionErrorTracker) {

        this.miruStats = miruStats;
        this.config = config;
        this.miruContextFactory = miruContextFactory;
        this.sipTrackerFactory = sipTrackerFactory;
        this.walClient = walClient;
        this.realtimeDelivery = realtimeDelivery;
        this.partitionEventHandler = partitionEventHandler;
        this.rebuildDirector = rebuildDirector;
        this.scheduledBoostrapExecutor = scheduledBoostrapExecutor;
        this.scheduledRebuildExecutor = scheduledRebuildExecutor;
        this.scheduledSipMigrateExecutor = scheduledSipMigrateExecutor;
        this.rebuildExecutors = rebuildExecutors;
        this.sipIndexExecutor = sipIndexExecutor;
        this.persistentMergeExecutor = persistentMergeExecutor;
        this.transientMergeExecutor = transientMergeExecutor;
        this.rebuildIndexerThreads = rebuildIndexerThreads;
        this.indexCallbacks = indexCallbacks;
        this.indexRepairs = indexRepairs;
        this.persistentMergeChits = persistentMergeChits;
        this.transientMergeChits = transientMergeChits;
        this.partitionErrorTracker = partitionErrorTracker;
    }

    public <BM extends IBM, IBM> MiruLocalHostedPartition<BM, IBM, C, S> create(MiruBitmaps<BM, IBM> bitmaps,
        MiruPartitionCoord coord,
        long expireAfterMillis) throws Exception {

        return new MiruLocalHostedPartition<>(miruStats,
            bitmaps,
            partitionErrorTracker.track(coord),
            coord,
            expireAfterMillis > 0 ? System.currentTimeMillis() + expireAfterMillis : -1,
            miruContextFactory,
            sipTrackerFactory,
            walClient,
            realtimeDelivery,
            partitionEventHandler,
            rebuildDirector,
            scheduledBoostrapExecutor,
            scheduledRebuildExecutor,
            scheduledSipMigrateExecutor,
            rebuildExecutors,
            sipIndexExecutor,
            persistentMergeExecutor,
            transientMergeExecutor,
            rebuildIndexerThreads,
            indexCallbacks,
            indexRepairs,
            new MiruIndexer<>(
                new MiruIndexAuthz<>(),
                new MiruIndexPrimaryFields<>(),
                new MiruIndexValueBits<>(),
                new MiruIndexBloom<>(new BloomIndex<>(bitmaps, Hashing.murmur3_128(), 100_000, 0.01f)),
                new MiruIndexLatest<>(),
                new MiruIndexPairedLatest<>()),
            config.getPartitionAllowNonLatestSchemaInteractions(),
            config.getPartitionCompactOnClosedWriters(),
            config.getPartitionRebuildBatchSize(),
            config.getPartitionSipBatchSize(),
            persistentMergeChits,
            transientMergeChits,
            new MiruLocalHostedPartition.Timings(
                config.getPartitionBootstrapIntervalInMillis(),
                config.getPartitionRebuildIntervalInMillis(),
                config.getPartitionSipMigrateIntervalInMillis(),
                config.getPartitionBanUnregisteredSchemaMillis(),
                config.getPartitionMigrationWaitInMillis(),
                config.getPartitionSipNotifyEndOfStreamMillis(),
                config.getPartitionRebuildEstimateActivityCountIntervalInMillis(),
                config.getPartitionCyaSipIntervalInMillis()));
    }

    public void prioritizeRebuild(MiruLocalHostedPartition<?, ?, ?, ?> partition) {
        rebuildDirector.prioritize(partition);
    }
}
