package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.rcvs.api.timestamper.Timestamper;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author jonathan
 */
public class MiruLocalPartitionFactory {

    private final Timestamper timestamper;
    private final MiruServiceConfig config;
    private final MiruContextFactory miruContextFactory;
    private final MiruActivityWALReader activityWALReader;
    private final MiruPartitionEventHandler partitionEventHandler;
    private final ByteBufferFactory byteBufferFactory;
    private final ScheduledExecutorService scheduledBoostrapExecutor;
    private final ScheduledExecutorService scheduledRebuildExecutor;
    private final ScheduledExecutorService scheduledSipMigrateExecutor;
    private final ExecutorService rebuildExecutors;
    private final ExecutorService rebuildIndexExecutor;
    private final ExecutorService sipIndexExecutor;

    public MiruLocalPartitionFactory(Timestamper timestamper,
            MiruServiceConfig config,
            MiruContextFactory miruContextFactory,
            MiruActivityWALReader activityWALReader,
            MiruPartitionEventHandler partitionEventHandler,
            ByteBufferFactory byteBufferFactory,
            ScheduledExecutorService scheduledBoostrapExecutor,
            ScheduledExecutorService scheduledRebuildExecutor,
            ScheduledExecutorService scheduledSipMigrateExecutor,
            ExecutorService rebuildExecutors,
            ExecutorService rebuildIndexExecutor,
            ExecutorService sipIndexExecutor) {
        this.timestamper = timestamper;
        this.config = config;
        this.miruContextFactory = miruContextFactory;
        this.activityWALReader = activityWALReader;
        this.partitionEventHandler = partitionEventHandler;
        this.byteBufferFactory = byteBufferFactory;
        this.scheduledBoostrapExecutor = scheduledBoostrapExecutor;
        this.scheduledRebuildExecutor = scheduledRebuildExecutor;
        this.scheduledSipMigrateExecutor = scheduledSipMigrateExecutor;
        this.rebuildExecutors = rebuildExecutors;
        this.rebuildIndexExecutor = rebuildIndexExecutor;
        this.sipIndexExecutor = sipIndexExecutor;
    }

    public <BM> MiruHostedPartition<BM> create(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord) throws Exception {
        return new MiruLocalHostedPartition<>(timestamper, bitmaps, coord, miruContextFactory,
                activityWALReader,
                partitionEventHandler,
                byteBufferFactory,
                scheduledBoostrapExecutor,
                scheduledRebuildExecutor,
                scheduledSipMigrateExecutor,
                rebuildExecutors,
                rebuildIndexExecutor,
                sipIndexExecutor,
                config.getPartitionWakeOnIndex(),
                config.getPartitionRebuildBatchSize(),
                config.getPartitionSipBatchSize(),
                config.getPartitionRebuildFailureSleepMillis(),
                config.getPartitionBootstrapIntervalInMillis(),
                config.getPartitionRunnableIntervalInMillis(),
                config.getPartitionBanUnregisteredSchemaMillis());
    }
}
