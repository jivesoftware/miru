package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.jivesoftware.os.jive.utils.health.api.HealthCounter;
import com.jivesoftware.os.jive.utils.health.api.HealthFactory;
import com.jivesoftware.os.jive.utils.health.api.MinMaxHealthCheckConfig;
import com.jivesoftware.os.jive.utils.health.api.MinMaxHealthChecker;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionCoordMetrics;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaUnvailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.service.stream.MiruIndexer;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.rcvs.api.timestamper.Timestamper;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

/**
 * @author jonathan
 */
public class MiruLocalHostedPartition<BM> implements MiruHostedPartition<BM> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final Timestamper timestamper;
    private final MiruBitmaps<BM> bitmaps;
    private final MiruPartitionCoord coord;
    private final MiruContextFactory streamFactory;
    private final MiruActivityWALReader activityWALReader;
    private final MiruPartitionEventHandler partitionEventHandler;
    private final AtomicLong sizeInMemoryBytes = new AtomicLong();
    private final AtomicLong sizeOnDiskBytes = new AtomicLong();
    private final AtomicLong sizeInMemoryExpiresAfter = new AtomicLong();
    private final AtomicLong sizeOnDiskExpiresAfter = new AtomicLong();
    private final AtomicBoolean removed = new AtomicBoolean(false);

    private final Collection<ScheduledFuture<?>> futures;
    private final ScheduledExecutorService scheduledRebuildExecutor;
    private final ScheduledExecutorService scheduledSipMigrateExecutor;
    private final ExecutorService hbaseRebuildExecutors;
    private final ExecutorService rebuildIndexExecutor;
    private final ExecutorService sipIndexExecutor;
    private final MiruIndexRepairs indexRepairs;
    private final MiruIndexer<BM> indexer;
    private final boolean partitionWakeOnIndex;
    private final int partitionRebuildBatchSize;
    private final int partitionSipBatchSize;
    private final long partitionRebuildFailureSleepMillis;
    private final long partitionRunnableIntervalInMillis;
    private final long partitionBanUnregisteredSchemaMillis;
    private final long partitionMigrationWaitInMillis;
    private final long maxSipClockSkew;
    private final int maxSipReplaySize;

    private final AtomicReference<MiruPartitionAccessor<BM>> accessorRef = new AtomicReference<>();
    private final Object factoryLock = new Object();

    private interface BootstrapCount extends MinMaxHealthCheckConfig {
        @StringDefault("rebuild>pending")
        @Override
        String getName();

        @StringDefault("Too many pending rebuilds.")
        @Override
        String getUnhealthyMessage();

        @LongDefault(0)
        @Override
        Long getMin();

        @LongDefault(1_000)
        @Override
        Long getMax();
    }

    private static final HealthCounter bootstrapCounter = HealthFactory.getHealthCounter(BootstrapCount.class, MinMaxHealthChecker.FACTORY);

    public MiruLocalHostedPartition(
        Timestamper timestamper,
        MiruBitmaps<BM> bitmaps,
        MiruPartitionCoord coord,
        MiruContextFactory streamFactory,
        MiruActivityWALReader activityWALReader,
        MiruPartitionEventHandler partitionEventHandler,
        ScheduledExecutorService scheduledBootstrapExecutor,
        ScheduledExecutorService scheduledRebuildExecutor,
        ScheduledExecutorService scheduledSipMigrateExecutor,
        ExecutorService hbaseRebuildExecutors,
        ExecutorService rebuildIndexExecutor,
        ExecutorService sipIndexExecutor,
        MiruIndexRepairs indexRepairs,
        MiruIndexer<BM> indexer,
        boolean partitionWakeOnIndex,
        int partitionRebuildBatchSize,
        int partitionSipBatchSize,
        long partitionRebuildFailureSleepMillis,
        long partitionBootstrapIntervalInMillis,
        long partitionRunnableIntervalInMillis,
        long partitionBanUnregisteredSchemaMillis)
        throws Exception {

        this.timestamper = timestamper;
        this.bitmaps = bitmaps;
        this.coord = coord;
        this.streamFactory = streamFactory;
        this.activityWALReader = activityWALReader;
        this.partitionEventHandler = partitionEventHandler;
        this.scheduledRebuildExecutor = scheduledRebuildExecutor;
        this.scheduledSipMigrateExecutor = scheduledSipMigrateExecutor;
        this.hbaseRebuildExecutors = hbaseRebuildExecutors;
        this.rebuildIndexExecutor = rebuildIndexExecutor;
        this.sipIndexExecutor = sipIndexExecutor;
        this.indexRepairs = indexRepairs;
        this.indexer = indexer;
        this.partitionWakeOnIndex = partitionWakeOnIndex;
        this.partitionRebuildBatchSize = partitionRebuildBatchSize;
        this.partitionSipBatchSize = partitionSipBatchSize;
        this.partitionRebuildFailureSleepMillis = partitionRebuildFailureSleepMillis;
        this.partitionRunnableIntervalInMillis = partitionRunnableIntervalInMillis;
        this.partitionBanUnregisteredSchemaMillis = partitionBanUnregisteredSchemaMillis;
        this.partitionMigrationWaitInMillis = 3_000; //TODO config
        this.maxSipClockSkew = TimeUnit.SECONDS.toMillis(10); //TODO config
        this.maxSipReplaySize = 100; //TODO config

        MiruPartitionCoordInfo coordInfo = new MiruPartitionCoordInfo(MiruPartitionState.offline, streamFactory.findBackingStorage(coord));
        MiruPartitionAccessor<BM> accessor = new MiruPartitionAccessor<>(bitmaps, coord, coordInfo, null, 0, indexRepairs, indexer);
        this.accessorRef.set(accessor);
        log.incAtomic("state>" + accessor.info.state.name());

        scheduledBootstrapExecutor.scheduleWithFixedDelay(
            new BootstrapRunnable(), 0, partitionBootstrapIntervalInMillis, TimeUnit.MILLISECONDS);

        futures = Lists.newArrayListWithCapacity(2); // rebuild, sip-migrate
    }

    private MiruPartitionAccessor<BM> open(MiruPartitionAccessor<BM> accessor, MiruPartitionCoordInfo coordInfo) throws Exception {
        synchronized (factoryLock) {
            MiruPartitionState openingState;
            if (accessor.info.storage.isMemoryBacked()) {
                if (coordInfo.state == MiruPartitionState.offline) {
                    openingState = MiruPartitionState.offline;
                } else {
                    openingState = MiruPartitionState.bootstrap;
                }
            } else {
                openingState = MiruPartitionState.online;
            }

            Optional<MiruContext<BM>> optionalStream = Optional.absent();
            if (openingState != MiruPartitionState.offline && accessor.context == null) {
                MiruContext<BM> stream = streamFactory.allocate(bitmaps, coord, accessor.info.storage);
                optionalStream = Optional.of(stream);
            }
            MiruPartitionAccessor<BM> opened = new MiruPartitionAccessor<>(bitmaps, coord, coordInfo.copyToState(openingState), optionalStream.orNull(),
                streamFactory.getSip(coord), indexRepairs, indexer);
            opened = updatePartition(accessor, opened);
            if (opened != null) {
                clearFutures();
                futures.add(scheduledRebuildExecutor.scheduleWithFixedDelay(new RebuildIndexRunnable(),
                    0, partitionRunnableIntervalInMillis, TimeUnit.MILLISECONDS));
                futures.add(scheduledSipMigrateExecutor.scheduleWithFixedDelay(new SipMigrateIndexRunnable(),
                    0, partitionRunnableIntervalInMillis, TimeUnit.MILLISECONDS));
                return opened;
            } else {
                return accessor;
            }
        }
    }

    @Override
    public MiruRequestHandle<BM> getQueryHandle() throws Exception {
        MiruPartitionAccessor<BM> accessor = accessorRef.get();
        if (!removed.get() && accessor.needsHotDeploy()) {
            log.info("Hot deploying for query: {}", coord);
            accessor = open(accessor, accessor.info.copyToState(MiruPartitionState.online));
        }
        return accessor.getRequestHandle();
    }

    @Override
    public void remove() throws Exception {
        log.info("Removing partition by request: {}", coord);
        removed.set(true);
        close();
    }

    private void close() throws Exception {
        try {
            synchronized (factoryLock) {
                MiruPartitionAccessor<BM> accessor = accessorRef.get();
                MiruPartitionCoordInfo coordInfo = accessor.info.copyToState(MiruPartitionState.offline);
                MiruPartitionAccessor<BM> closed = new MiruPartitionAccessor<>(bitmaps, coord, coordInfo, null, 0, indexRepairs, indexer);
                closed = updatePartition(accessor, closed);
                if (closed != null) {
                    if (accessor.context != null) {
                        streamFactory.close(accessor.close());
                    }
                    clearFutures();
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to close partition", e);
        }
    }

    private void clearFutures() {
        for (ScheduledFuture<?> future : futures) {
            future.cancel(true);
        }
        futures.clear();
    }

    @Override
    public boolean isLocal() {
        return true;
    }

    @Override
    public MiruPartitionCoord getCoord() {
        return coord;
    }

    @Override
    public MiruPartitionId getPartitionId() {
        return coord.partitionId;
    }

    @Override
    public MiruPartitionState getState() {
        return accessorRef.get().info.state;
    }

    @Override
    public MiruBackingStorage getStorage() {
        return accessorRef.get().info.storage;
    }

    @Override
    public MiruTenantId getTenantId() {
        return coord.tenantId;
    }

    @Override
    public void index(Iterator<MiruPartitionedActivity> partitionedActivities) throws Exception {
        // intentionally locking all stream writes for the entire batch to avoid getting a lock for each activity
        MiruPartitionAccessor<BM> accessor = accessorRef.get();
        if (accessor.isOpenForWrites()) {
            //TODO handle return case
            int count = accessor.indexInternal(partitionedActivities, MiruPartitionAccessor.IndexStrategy.ingress, MoreExecutors.sameThreadExecutor());
            if (count > 0) {
                log.inc("indexIngress>written", count);
            }
        } else {
            int count = 0;
            while (partitionedActivities.hasNext()) {
                MiruPartitionedActivity partitionedActivity = partitionedActivities.next();
                if (partitionedActivity.partitionId.equals(coord.partitionId)) {
                    partitionedActivities.remove();
                    count++;
                }
            }
            log.inc("indexIngress>dropped", count);
        }

        if (partitionWakeOnIndex) {
            accessor.markForRefresh();
        }
    }

    @Override
    public void warm() {
        accessorRef.get().markForRefresh();
    }

    @Override
    public long sizeInMemory() throws Exception {
        long expiresAfter = sizeInMemoryExpiresAfter.get();
        MiruPartitionAccessor<BM> accessor = accessorRef.get();
        if (timestamper.get() > expiresAfter && accessor.info.state == MiruPartitionState.online) {
            sizeInMemoryBytes.set(accessor.context != null ? accessor.context.sizeInMemory() : 0);
            sizeInMemoryExpiresAfter.set(timestamper.get() + TimeUnit.MINUTES.toMillis(1));
        }
        return sizeInMemoryBytes.get();
    }

    @Override
    public long sizeOnDisk() throws Exception {
        long expiresAfter = sizeOnDiskExpiresAfter.get();
        MiruPartitionAccessor<BM> accessor = accessorRef.get();
        if (timestamper.get() > expiresAfter && accessor.info.state == MiruPartitionState.online) {
            sizeOnDiskBytes.set(accessor.context != null ? accessor.context.sizeOnDisk() : 0);
            sizeOnDiskExpiresAfter.set(timestamper.get() + TimeUnit.MINUTES.toMillis(1));
        }
        return sizeOnDiskBytes.get();
    }

    @Override
    public void setStorage(MiruBackingStorage storage) throws Exception {
        updateStorage(accessorRef.get(), storage, true);
    }

    private boolean updateStorage(MiruPartitionAccessor<BM> accessor, MiruBackingStorage destinationStorage, boolean force) throws Exception {
        synchronized (factoryLock) {
            boolean updated = false;
            try (MiruMigrationHandle<BM> handle = accessor.getMigrationHandle(partitionMigrationWaitInMillis)) {
                // make sure the accessor didn't change while getting the handle, and that it's ready to migrate
                if (accessorRef.get() == accessor && handle.canMigrateTo(destinationStorage)) {
                    MiruBackingStorage existingStorage = accessor.info.storage;
                    if (existingStorage == destinationStorage && !force) {
                        log.warn("Partition at {} ignored request to migrate to same storage {}", coord, destinationStorage);

                    } else if (existingStorage.isMemoryBacked() && destinationStorage.isMemoryBacked()) {
                        MiruContext<BM> fromStream = handle.getStream();
                        if (existingStorage == destinationStorage || !existingStorage.isIdentical(destinationStorage)) {
                            // same memory storage, or non-identical memory storage, triggers a rebuild
                            MiruContext<BM> toStream = streamFactory.allocate(bitmaps, coord, destinationStorage);
                            MiruPartitionAccessor<BM> migrated = handle.migrated(toStream, Optional.of(destinationStorage),
                                Optional.of(MiruPartitionState.bootstrap), 0);
                            migrated = updatePartition(accessor, migrated);
                            if (migrated != null) {
                                if (fromStream != null) {
                                    streamFactory.close(fromStream);
                                }
                                streamFactory.markStorage(coord, destinationStorage);
                                updated = true;
                            } else {
                                log.warn("Partition at {} failed to migrate to {}", coord, destinationStorage);
                            }
                        } else {
                            // different but identical storage updates without a rebuild
                            MiruPartitionAccessor<BM> migrated = handle.migrated(fromStream, Optional.of(destinationStorage),
                                Optional.<MiruPartitionState>absent(), accessor.sipTimestamp.get());
                            migrated = updatePartition(accessor, migrated);
                            if (migrated != null) {
                                streamFactory.markStorage(coord, destinationStorage);
                                updated = true;
                            } else {
                                log.warn("Partition at {} failed to migrate to {}", coord, destinationStorage);
                            }
                        }
                    } else if (existingStorage.isDiskBacked() && destinationStorage.isMemoryBacked()) {
                        MiruContext<BM> fromStream = handle.getStream();
                        MiruContext<BM> toStream = streamFactory.allocate(bitmaps, coord, destinationStorage);
                        // transitioning to memory, need to bootstrap and rebuild
                        Optional<MiruPartitionState> migrateToState = (accessor.info.state == MiruPartitionState.offline)
                            ? Optional.<MiruPartitionState>absent()
                            : Optional.of(MiruPartitionState.bootstrap);
                        MiruPartitionAccessor<BM> migrated = handle.migrated(toStream, Optional.of(destinationStorage), migrateToState, 0);
                        migrated = updatePartition(accessor, migrated);
                        if (migrated != null) {
                            streamFactory.close(fromStream);
                            streamFactory.cleanDisk(coord);
                            streamFactory.markStorage(coord, destinationStorage);
                            updated = true;
                        } else {
                            log.warn("Partition at {} failed to migrate to {}", coord, destinationStorage);
                            streamFactory.close(toStream);
                            streamFactory.markStorage(coord, existingStorage);
                        }

                    } else if (existingStorage.isMemoryBacked() && destinationStorage.isDiskBacked()) {
                        streamFactory.cleanDisk(coord);

                        MiruContext<BM> fromStream = handle.getStream();
                        MiruContext<BM> toStream;
                        if (destinationStorage == MiruBackingStorage.mem_mapped) {
                            toStream = streamFactory.copy(bitmaps, coord, fromStream, MiruBackingStorage.mem_mapped);
                        } else {
                            toStream = streamFactory.copy(bitmaps, coord, fromStream, MiruBackingStorage.disk);
                        }

                        streamFactory.markSip(coord, accessor.sipTimestamp.get());
                        MiruPartitionAccessor<BM> migrated = handle.migrated(toStream, Optional.of(destinationStorage), Optional.<MiruPartitionState>absent(),
                            accessor.sipTimestamp.get());

                        migrated = updatePartition(accessor, migrated);
                        if (migrated != null) {
                            streamFactory.close(fromStream);
                            streamFactory.markStorage(coord, destinationStorage);
                            updated = true;
                        } else {
                            log.warn("Partition at {} failed to migrate to {}, attempting to rewind", coord, destinationStorage);
                            streamFactory.close(toStream);
                            streamFactory.cleanDisk(coord);
                            streamFactory.markStorage(coord, existingStorage);
                        }

                    } else if (existingStorage.isDiskBacked() && destinationStorage.isDiskBacked()) {
                        //TODO check existingStorage.isIdentical(destinationStorage), else rebuild and somehow mark for migration to destinationStorage
                        // rely on the fact that the underlying file structure is identical for disk-backed storage types
                        MiruContext<BM> fromStream = handle.getStream();
                        MiruContext<BM> toStream = streamFactory.allocate(bitmaps, coord, destinationStorage);
                        MiruPartitionAccessor<BM> migrated = handle.migrated(toStream, Optional.of(destinationStorage), Optional.<MiruPartitionState>absent(),
                            accessor.sipTimestamp.get());

                        migrated = updatePartition(accessor, migrated);
                        if (migrated != null) {
                            streamFactory.close(fromStream);
                            streamFactory.markStorage(coord, destinationStorage);
                            updated = true;
                        } else {
                            log.info("Partition at {} failed to migrate to {}", coord, destinationStorage);
                            streamFactory.close(toStream);
                            streamFactory.markStorage(coord, existingStorage);
                        }

                    } else {
                        log.warn("Partition at {} ignored unsupported storage migration {} to {}", coord, existingStorage, destinationStorage);
                    }
                }
            }
            return updated;
        }
    }

    private MiruPartitionAccessor<BM> updatePartition(MiruPartitionAccessor<BM> existing, MiruPartitionAccessor<BM> update) {
        synchronized (accessorRef) {
            if (accessorRef.get() != existing) {
                return null;
            }

            try {
                MiruContext<BM> context = streamFactory.stateChanged(bitmaps, coord, update.context, update.info.storage, update.info.state);
                update = update.copyToContext(context);
            } catch (Exception e) {
                log.error("Failed to notify state change, aborting", e);
                return null;
            }

            Optional<Long> refreshTimestamp = Optional.absent();
            if (update.info.state != MiruPartitionState.offline) {
                refreshTimestamp = Optional.of(timestamper.get());
            }
            update.refreshTimestamp.set(refreshTimestamp);

            accessorRef.set(update);

            log.decAtomic("state>" + existing.info.state.name());
            log.incAtomic("state>" + update.info.state.name());
            if (existing.info.state != MiruPartitionState.bootstrap && update.info.state == MiruPartitionState.bootstrap) {
                bootstrapCounter.inc("Too many pending rebuilds.");
            } else if (existing.info.state == MiruPartitionState.bootstrap && update.info.state != MiruPartitionState.bootstrap) {
                bootstrapCounter.dec("Too many pending rebuilds.");
            }

            log.info("Partition is now {}/{} for {}", update.info.state, update.info.storage, coord);
            return update;
        }
    }

    protected class BootstrapRunnable implements Runnable {

        private final AtomicLong banUnregisteredSchema = new AtomicLong();

        @Override
        public void run() {
            try {
                try {
                    refreshTopology();
                } catch (Throwable t) {
                    log.error("RefreshTopology encountered a problem", t);
                }

                try {
                    checkActive();
                } catch (MiruSchemaUnvailableException sue) {
                    log.warn("Tenant is active but schema not available for {}", coord.tenantId);
                    log.debug("Tenant is active but schema not available", sue);
                } catch (Throwable t) {
                    log.error("CheckActive encountered a problem", t);
                }
            } catch (Throwable t) {
                log.error("Bootstrap encountered a problem", t);
            }
        }

        private void refreshTopology() throws Exception {
            MiruPartitionAccessor<BM> accessor = accessorRef.get();
            Optional<Long> timestamp = accessor.refreshTimestamp.getAndSet(null);
            if (timestamp != null) {
                MiruPartitionCoordMetrics metrics = new MiruPartitionCoordMetrics(sizeInMemory(), sizeOnDisk());
                partitionEventHandler.updateTopology(coord, Optional.of(accessor.info), metrics, timestamp);
            }
        }

        private void checkActive() throws Exception {
            if (removed.get()) {
                return;
            }

            MiruPartitionAccessor<BM> accessor = accessorRef.get();
            if (partitionEventHandler.isCoordActive(coord) && banUnregisteredSchema.get() < System.currentTimeMillis()) {
                if (accessor.info.state == MiruPartitionState.offline) {
                    try {
                        open(accessor, new MiruPartitionCoordInfo(MiruPartitionState.bootstrap, accessor.info.storage));
                    } catch (MiruPartitionUnavailableException e) {
                        log.warn("Partition is active for tenant {} but no schema is registered, banning for {} ms",
                            coord.tenantId, partitionBanUnregisteredSchemaMillis);
                        banUnregisteredSchema.set(System.currentTimeMillis() + partitionBanUnregisteredSchemaMillis);
                    }
                }
            } else {
                if (accessor.info.state != MiruPartitionState.offline) {
                    close();
                }
            }
        }

    }

    protected class RebuildIndexRunnable implements Runnable {

        @Override
        public void run() {
            try {
                MiruPartitionAccessor<BM> accessor = accessorRef.get();
                MiruPartitionState state = accessor.info.state;
                if (state == MiruPartitionState.bootstrap || state == MiruPartitionState.rebuilding) {
                    MiruPartitionAccessor<BM> rebuilding = accessor.copyToState(MiruPartitionState.rebuilding);
                    rebuilding = updatePartition(accessor, rebuilding);
                    if (rebuilding != null) {
                        try {
                            if (rebuild(rebuilding)) {
                                MiruPartitionAccessor<BM> online = rebuilding.copyToState(MiruPartitionState.online);
                                updatePartition(rebuilding, online);
                            }
                        } catch (Throwable t) {
                            log.error("Rebuild encountered a problem", t);
                        }
                    }
                }
            } catch (Throwable t) {
                log.error("RebuildIndex encountered a problem", t);
            }
        }

        private boolean rebuild(final MiruPartitionAccessor<BM> accessor) throws Exception {
            final ArrayBlockingQueue<List<MiruPartitionedActivity>> queue = new ArrayBlockingQueue<>(1);
            final AtomicLong rebuildTimestamp = new AtomicLong(accessor.rebuildTimestamp.get());
            final AtomicLong sipTimestamp = new AtomicLong(accessor.sipTimestamp.get());

            hbaseRebuildExecutors.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        final AtomicReference<List<MiruPartitionedActivity>> batchRef = new AtomicReference<List<MiruPartitionedActivity>>(
                            Lists.<MiruPartitionedActivity>newArrayListWithCapacity(partitionRebuildBatchSize));
                        activityWALReader.stream(coord.tenantId, coord.partitionId, accessor.rebuildTimestamp.get(), partitionRebuildBatchSize,
                            partitionRebuildFailureSleepMillis,
                            new MiruActivityWALReader.StreamMiruActivityWAL() {
                                private List<MiruPartitionedActivity> batch = batchRef.get();

                                @Override
                                public boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                                    batch.add(partitionedActivity);
                                    if (batch.size() == partitionRebuildBatchSize) {
                                        queue.put(batch);
                                        batch = Lists.newArrayListWithCapacity(partitionRebuildBatchSize);
                                        batchRef.set(batch);
                                    }

                                    // stop if the accessor has changed
                                    return accessorRef.get() == accessor;
                                }
                            }
                        );

                        List<MiruPartitionedActivity> batch = batchRef.get();
                        if (!batch.isEmpty()) {
                            queue.put(batch);
                        }

                        // signals end of rebuild
                        queue.put(Collections.<MiruPartitionedActivity>emptyList());
                    } catch (Exception x) {
                        log.error("Failure while rebuilding {}", new Object[] { coord }, x);
                    }
                }
            });

            List<MiruPartitionedActivity> partitionedActivities;
            while (true) {
                partitionedActivities = queue.take();
                if (partitionedActivities.isEmpty()) {
                    // end of rebuild
                    break;
                }

                for (int i = partitionedActivities.size() - 1; i > -1; i--) {
                    MiruPartitionedActivity partitionedActivity = partitionedActivities.get(i);
                    // only adjust timestamps for activity types
                    if (partitionedActivity.type.isActivityType()) {
                        // rebuild offset is based on the activity timestamp
                        if (partitionedActivity.timestamp > rebuildTimestamp.get()) {
                            rebuildTimestamp.set(partitionedActivity.timestamp);
                        }

                        // activityWAL uses CurrentTimestamper, so column version implies desired sip offset
                        if (partitionedActivity.clockTimestamp > sipTimestamp.get()) {
                            sipTimestamp.set(partitionedActivity.clockTimestamp);
                        }
                        break;
                    }
                }

                log.startTimer("rebuild>batchSize-" + partitionRebuildBatchSize);
                int count = partitionedActivities.size();
                try {
                    accessor.indexInternal(partitionedActivities.iterator(), MiruPartitionAccessor.IndexStrategy.rebuild, rebuildIndexExecutor);
                    accessor.rebuildTimestamp.set(rebuildTimestamp.get());
                    accessor.sipTimestamp.set(sipTimestamp.get());
                } finally {
                    log.stopTimer("rebuild>batchSize-" + partitionRebuildBatchSize);
                    log.inc("rebuild", count);
                    log.inc("rebuild>tenant>" + coord.tenantId, count);
                    log.inc("rebuild>tenant>" + coord.tenantId + ">partition>" + coord.partitionId, count);
                }
            }

            return accessorRef.get() == accessor;
        }
    }

    protected class SipMigrateIndexRunnable implements Runnable {

        @Override
        public void run() {
            try {
                MiruPartitionAccessor<BM> accessor = accessorRef.get();
                MiruPartitionState state = accessor.info.state;
                if (state == MiruPartitionState.online) {
                    try {
                        sip(accessor);
                    } catch (Throwable t) {
                        log.error("Sip encountered a problem", t);
                    }
                    try {
                        migrate(accessor);
                    } catch (Throwable t) {
                        log.error("Migrate encountered a problem", t);
                    }
                }
            } catch (Throwable t) {
                log.error("SipMigrateIndex encountered a problem", t);
            }
        }

        private boolean sip(final MiruPartitionAccessor<BM> accessor) throws Exception {
            if (!accessor.isOpenForWrites()) {
                return false;
            }

            final MiruSipTracker sipTracker = new MiruSipTracker(maxSipReplaySize, maxSipClockSkew, accessor.seenLastSip.get());

            long afterTimestamp = accessor.sipTimestamp.get();
            final List<MiruPartitionedActivity> partitionedActivities = Lists.newLinkedList();
            activityWALReader.streamSip(coord.tenantId, coord.partitionId, afterTimestamp, partitionSipBatchSize, partitionRebuildFailureSleepMillis,
                new MiruActivityWALReader.StreamMiruActivityWAL() {
                    @Override
                    public boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                        long version = partitionedActivity.activity.isPresent() ? partitionedActivity.activity.get().version : 0;
                        TimeAndVersion timeAndVersion = new TimeAndVersion(partitionedActivity.timestamp, version);

                        if (partitionedActivity.type.isBoundaryType() || !sipTracker.wasSeenLastSip(timeAndVersion)) {
                            partitionedActivities.add(partitionedActivity);
                        }
                        sipTracker.addSeenThisSip(timeAndVersion);

                        if (!partitionedActivity.type.isBoundaryType()) {
                            sipTracker.put(partitionedActivity.clockTimestamp);
                        }

                        // stop if the accessor has changed
                        return accessorRef.get() == accessor;
                    }
                }
            );
            accessor.indexInternal(partitionedActivities.iterator(), MiruPartitionAccessor.IndexStrategy.sip, sipIndexExecutor);

            long suggestedTimestamp = sipTracker.suggestTimestamp(afterTimestamp);
            boolean sipUpdated = accessor.sipTimestamp.compareAndSet(afterTimestamp, suggestedTimestamp);
            if (sipUpdated) {
                if (accessor.info.storage.isDiskBacked()) {
                    streamFactory.markSip(coord, suggestedTimestamp);
                }
                accessor.seenLastSip.compareAndSet(sipTracker.getSeenLastSip(), sipTracker.getSeenThisSip());
            }

            return accessorRef.get() == accessor;
        }

        private boolean migrate(MiruPartitionAccessor<BM> accessor) throws Exception {
            return accessor.canAutoMigrate() && updateStorage(accessor, MiruBackingStorage.mem_mapped, false);
        }

    }

    @Override
    public String toString() {
        return "MiruLocalHostedPartition{"
            + "coord=" + coord
            + ", accessorRef=" + accessorRef.get()
            + '}';
    }

}
