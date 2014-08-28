package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.query.MiruBitmaps;
import com.jivesoftware.os.miru.query.MiruHostedPartition;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.stream.MiruStreamFactory;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author jonathan
 */
public class MiruLocalPartitionFactory {

    private final MiruServiceConfig config;
    private final MiruStreamFactory miruStreamFactory;
    private final MiruActivityWALReader activityWALReader;
    private final MiruPartitionEventHandler partitionEventHandler;
    private final ScheduledExecutorService scheduledExecutorService;

    public MiruLocalPartitionFactory(MiruServiceConfig config,
            MiruStreamFactory miruStreamFactory,
            MiruActivityWALReader activityWALReader,
            MiruPartitionEventHandler partitionEventHandler,
            ScheduledExecutorService scheduledExecutorService) {
        this.config = config;
        this.miruStreamFactory = miruStreamFactory;
        this.activityWALReader = activityWALReader;
        this.partitionEventHandler = partitionEventHandler;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public <BM> MiruHostedPartition<BM> create(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord) throws Exception {
        return new MiruLocalHostedPartition<>(bitmaps, coord, miruStreamFactory,
                activityWALReader, partitionEventHandler, scheduledExecutorService, config.getPartitionRebuildBatchSize(),
                config.getPartitionBootstrapIntervalInMillis(), config.getPartitionRunnableIntervalInMillis());
    }
}
