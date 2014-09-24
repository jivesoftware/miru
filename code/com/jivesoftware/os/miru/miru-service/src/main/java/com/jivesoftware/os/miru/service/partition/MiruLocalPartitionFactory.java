package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.Timestamper;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
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
    private final ScheduledExecutorService scheduledExecutorService;

    public MiruLocalPartitionFactory(Timestamper timestamper,
        MiruServiceConfig config,
        MiruContextFactory miruContextFactory,
        MiruActivityWALReader activityWALReader,
        MiruPartitionEventHandler partitionEventHandler,
        ScheduledExecutorService scheduledExecutorService) {
        this.timestamper = timestamper;
        this.config = config;
        this.miruContextFactory = miruContextFactory;
        this.activityWALReader = activityWALReader;
        this.partitionEventHandler = partitionEventHandler;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public <BM> MiruHostedPartition<BM> create(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord) throws Exception {
        return new MiruLocalHostedPartition<>(timestamper, bitmaps, coord, miruContextFactory,
            activityWALReader, partitionEventHandler, scheduledExecutorService, config.getPartitionWakeOnIndex(), config.getPartitionRebuildBatchSize(),
            config.getPartitionBootstrapIntervalInMillis(), config.getPartitionRunnableIntervalInMillis());
    }
}
