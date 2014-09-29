package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.Timestamper;
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
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaUnvailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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

    private final Collection<Runnable> runnables;
    private final Collection<ScheduledFuture<?>> futures;
    private final ScheduledExecutorService scheduledExecutorService;
    private final boolean partitionWakeOnIndex;
    private final int partitionRebuildBatchSize;
    private final long partitionRunnableIntervalInMillis;
    private final long partitionMigrationWaitInMillis;
    private final long maxSipClockSkew;
    private final int maxSipReplaySize;

    private final AtomicReference<MiruPartitionAccessor<BM>> accessorRef = new AtomicReference<>();
    private final Object factoryLock = new Object();

    public MiruLocalHostedPartition(
        Timestamper timestamper,
        MiruBitmaps<BM> bitmaps,
        MiruPartitionCoord coord,
        MiruContextFactory streamFactory,
        MiruActivityWALReader activityWALReader,
        MiruPartitionEventHandler partitionEventHandler,
        ScheduledExecutorService scheduledExecutorService,
        boolean partitionWakeOnIndex,
        int partitionRebuildBatchSize,
        long partitionBootstrapIntervalInMillis,
        long partitionRunnableIntervalInMillis)
        throws Exception {

        this.timestamper = timestamper;
        this.bitmaps = bitmaps;
        this.coord = coord;
        this.streamFactory = streamFactory;
        this.activityWALReader = activityWALReader;
        this.partitionEventHandler = partitionEventHandler;
        this.scheduledExecutorService = scheduledExecutorService;
        this.partitionWakeOnIndex = partitionWakeOnIndex;
        this.partitionRebuildBatchSize = partitionRebuildBatchSize;
        this.partitionRunnableIntervalInMillis = partitionRunnableIntervalInMillis;
        this.partitionMigrationWaitInMillis = 3_000; //TODO config
        this.maxSipClockSkew = TimeUnit.SECONDS.toMillis(10); //TODO config
        this.maxSipReplaySize = 100; //TODO config

        MiruPartitionCoordInfo coordInfo = new MiruPartitionCoordInfo(MiruPartitionState.offline, streamFactory.findBackingStorage(coord));
        MiruPartitionAccessor<BM> accessor = new MiruPartitionAccessor<>(bitmaps, coord, coordInfo, null, 0);
        this.accessorRef.set(accessor);

        scheduledExecutorService.scheduleWithFixedDelay(
            new BootstrapRunnable(), 0, partitionBootstrapIntervalInMillis, TimeUnit.MILLISECONDS);

        runnables = ImmutableList.<Runnable>of(new ManageIndexRunnable());
        futures = Lists.newArrayListWithCapacity(runnables.size());
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
                streamFactory.getSip(coord));
            if (updatePartition(accessor, opened)) {
                clearFutures();
                for (Runnable runnable : runnables) {
                    ScheduledFuture<?> future = scheduledExecutorService.scheduleWithFixedDelay(
                        runnable, 0, partitionRunnableIntervalInMillis, TimeUnit.MILLISECONDS);
                    futures.add(future);
                }
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
                MiruPartitionAccessor<BM> closed = new MiruPartitionAccessor<>(bitmaps, coord, coordInfo, null, 0);
                if (updatePartition(accessor, closed)) {
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
            accessor.indexInternal(partitionedActivities, MiruPartitionAccessor.IndexStrategy.ingress);
        } else {
            while (partitionedActivities.hasNext()) {
                MiruPartitionedActivity partitionedActivity = partitionedActivities.next();
                if (partitionedActivity.partitionId.equals(coord.partitionId)) {
                    partitionedActivities.remove();
                }
            }
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
                            if (updatePartition(accessor, migrated)) {
                                streamFactory.close(fromStream);
                                streamFactory.markStorage(coord, destinationStorage);
                                updated = true;
                            } else {
                                log.warn("Partition at {} failed to migrate to {}", coord, destinationStorage);
                            }
                        } else {
                            // different but identical storage updates without a rebuild
                            MiruPartitionAccessor<BM> migrated = handle.migrated(fromStream, Optional.of(destinationStorage),
                                Optional.<MiruPartitionState>absent(), accessor.sipTimestamp.get());
                            if (updatePartition(accessor, migrated)) {
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
                        if (updatePartition(accessor, migrated)) {
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
                            toStream = streamFactory.copyMemMapped(bitmaps, coord, fromStream);
                        } else {
                            toStream = streamFactory.copyToDisk(bitmaps, coord, fromStream);
                        }

                        streamFactory.markSip(coord, accessor.sipTimestamp.get());
                        MiruPartitionAccessor<BM> migrated = handle.migrated(toStream, Optional.of(destinationStorage), Optional.<MiruPartitionState>absent(),
                            accessor.sipTimestamp.get());

                        if (updatePartition(accessor, migrated)) {
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

                        if (updatePartition(accessor, migrated)) {
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

    private boolean updatePartition(MiruPartitionAccessor<BM> existing, MiruPartitionAccessor<BM> update) throws Exception {
        if (accessorRef.compareAndSet(existing, update)) {
            Optional<Long> refreshTimestamp = Optional.absent();
            if (update.info.state != MiruPartitionState.offline) {
                refreshTimestamp = Optional.of(timestamper.get());
            }
            MiruPartitionCoordMetrics metrics = new MiruPartitionCoordMetrics(sizeInMemory(), sizeOnDisk());
            partitionEventHandler.partitionChanged(coord, update.info, metrics, refreshTimestamp);
            log.info("Partition is now {}/{} for {}", update.info.state, update.info.storage, coord);
            return true;
        }
        return false;
    }

    protected class BootstrapRunnable implements Runnable {

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
            long timestamp = accessorRef.get().refreshTimestamp.getAndSet(0);
            if (timestamp > 0) {
                MiruPartitionCoordMetrics metrics = new MiruPartitionCoordMetrics(sizeInMemory(), sizeOnDisk());
                partitionEventHandler.refreshTopology(coord, metrics, timestamp);
            }
        }

        private void checkActive() throws Exception {
            if (removed.get()) {
                return;
            }

            MiruPartitionAccessor<BM> accessor = accessorRef.get();
            if (partitionEventHandler.isCoordActive(coord)) {
                if (accessor.info.state == MiruPartitionState.offline) {
                    open(accessor, new MiruPartitionCoordInfo(MiruPartitionState.bootstrap, accessor.info.storage));
                }
            } else {
                if (accessor.info.state != MiruPartitionState.offline) {
                    close();
                }
            }
        }

    }

    protected class ManageIndexRunnable implements Runnable {

        @Override
        public void run() {
            try {
                MiruPartitionAccessor<BM> accessor = accessorRef.get();
                MiruPartitionState state = accessor.info.state;
                if (state == MiruPartitionState.offline) {
                    // do nothing
                } else if (state == MiruPartitionState.bootstrap || state == MiruPartitionState.rebuilding) {
                    MiruPartitionAccessor<BM> rebuilding = accessor.copyToState(MiruPartitionState.rebuilding);
                    if (updatePartition(accessor, rebuilding)) {
                        try {
                            if (rebuild(rebuilding)) {
                                MiruPartitionAccessor<BM> online = rebuilding.copyToState(MiruPartitionState.online);
                                updatePartition(rebuilding, online);
                            }
                        } catch (Throwable t) {
                            log.error("Rebuild encountered a problem", t);
                        }
                    }
                } else if (state == MiruPartitionState.online) {
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
                log.error("ManageIndex encountered a problem", t);
            }
        }

        private boolean rebuild(final MiruPartitionAccessor<BM> accessor) throws Exception {
            final List<MiruPartitionedActivity> partitionedActivities = Lists.newLinkedList();
            final AtomicLong rebuildTimestamp = new AtomicLong(accessor.rebuildTimestamp.get());
            final AtomicLong sipTimestamp = new AtomicLong(accessor.sipTimestamp.get());

            activityWALReader.stream(coord.tenantId, coord.partitionId, accessor.rebuildTimestamp.get(),
                new MiruActivityWALReader.StreamMiruActivityWAL() {
                    @Override
                    public boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                        partitionedActivities.add(partitionedActivity);

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
                        }

                        if (partitionedActivities.size() == partitionRebuildBatchSize) {
                            log.startTimer("rebuild>batchSize-" + partitionRebuildBatchSize);
                            try {
                                accessor.indexInternal(partitionedActivities.iterator(), MiruPartitionAccessor.IndexStrategy.rebuild);
                                accessor.rebuildTimestamp.set(rebuildTimestamp.get());
                                accessor.sipTimestamp.set(sipTimestamp.get());
                                // indexInternal inherently clears the list by removing elements from the iterator, but just to be safe
                                partitionedActivities.clear();
                            } finally {
                                log.stopTimer("rebuild>batchSize-" + partitionRebuildBatchSize);
                            }
                        }

                        // stop if the accessor has changed
                        return accessorRef.get() == accessor;
                    }
                }
            );

            if (!partitionedActivities.isEmpty()) {
                log.startTimer("rebuild>batchSize-" + partitionRebuildBatchSize);
                try {
                    accessor.indexInternal(partitionedActivities.iterator(), MiruPartitionAccessor.IndexStrategy.rebuild);
                    accessor.rebuildTimestamp.set(rebuildTimestamp.get());
                    accessor.sipTimestamp.set(sipTimestamp.get());
                } finally {
                    log.stopTimer("rebuild>batchSize-" + partitionRebuildBatchSize);
                }
            }

            return accessorRef.get() == accessor;
        }

        private boolean sip(final MiruPartitionAccessor<BM> accessor) throws Exception {
            if (!accessor.isOpenForWrites()) {
                return false;
            }

            final MiruSipTracker sipTracker = new MiruSipTracker(maxSipReplaySize, maxSipClockSkew, accessor.seenLastSip.get());

            long afterTimestamp = accessor.sipTimestamp.get();
            final List<MiruPartitionedActivity> partitionedActivities = Lists.newLinkedList();
            activityWALReader.streamSip(coord.tenantId, coord.partitionId, afterTimestamp,
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
            accessor.indexInternal(partitionedActivities.iterator(), MiruPartitionAccessor.IndexStrategy.sip);

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
