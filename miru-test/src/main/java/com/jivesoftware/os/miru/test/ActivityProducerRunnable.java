package com.jivesoftware.os.miru.test;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class ActivityProducerRunnable implements Runnable {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruTestActivityDistributor activityDistributor;
    private final MiruTestFeatureSupplier featureSupplier;
    private final int numPartitions;
    private final boolean closeFinalPartition;
    private final int writerId;
    private final BlockingQueue<MiruPartitionedActivity> queue;
    private final AtomicInteger index;
    private final Optional<MiruTestActivityDistributor.Revisitor> revisitor;
    private final AtomicBoolean done;

    public ActivityProducerRunnable(MiruTestActivityDistributor activityDistributor, MiruTestFeatureSupplier featureSupplier,
        int numPartitions, boolean closeFinalPartition, int writerId, BlockingQueue<MiruPartitionedActivity> queue, AtomicInteger index,
        Optional<MiruTestActivityDistributor.Revisitor> revisitor, AtomicBoolean done) {
        this.activityDistributor = activityDistributor;
        this.featureSupplier = featureSupplier;
        this.numPartitions = numPartitions;
        this.closeFinalPartition = closeFinalPartition;
        this.writerId = writerId;
        this.queue = queue;
        this.index = index;
        this.revisitor = revisitor;
        this.done = done;
    }

    @Override
    public void run() {
        try {
            final AtomicLong timestamps = new AtomicLong(System.currentTimeMillis() - activityDistributor.getTotalActivities());
            MiruPartitionedActivityFactory factory = new MiruPartitionedActivityFactory(timestamps::getAndIncrement);

            MiruTenantId tenantId = featureSupplier.miruTenantId();

            final boolean background = revisitor.isPresent();
            final int partitionsToWrite = background ? 1 : numPartitions;
            final int activitiesPerPartition = activityDistributor.getTotalActivities() / partitionsToWrite;
            final int startingPartition;
            if (background) {
                startingPartition = closeFinalPartition ? numPartitions : numPartitions - 1;
            } else {
                startingPartition = 0;
            }

            for (int p = startingPartition; p < startingPartition + partitionsToWrite; p++) {
                log.info("Begin partition {}", p);
                MiruPartitionId partitionId = MiruPartitionId.of(p);
                queue.put(factory.begin(writerId, partitionId, tenantId, index.get() + activitiesPerPartition));
                for (int i = 0; i < activitiesPerPartition; i++) {
                    queue.put(activityDistributor.distribute(factory, writerId, partitionId, index.incrementAndGet(), revisitor));
                }
                if (!background && (p < numPartitions - 1 || closeFinalPartition)) {
                    index.set(0);
                    queue.put(factory.end(writerId, partitionId, tenantId, index.get() + activitiesPerPartition));
                    log.info("End partition {}", p);
                }
            }
        } catch (InterruptedException | IOException e) {
            log.error("Activity producer died", e);
        }

        done.compareAndSet(false, true);
    }
}
