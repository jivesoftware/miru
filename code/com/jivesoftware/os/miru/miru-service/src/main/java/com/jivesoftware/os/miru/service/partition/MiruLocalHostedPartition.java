package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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
import com.jivesoftware.os.miru.query.MiruBitmaps;
import com.jivesoftware.os.miru.query.MiruHostedPartition;
import com.jivesoftware.os.miru.query.MiruQueryHandle;
import com.jivesoftware.os.miru.service.stream.MiruStream;
import com.jivesoftware.os.miru.service.stream.MiruStreamFactory;
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

    private final MiruBitmaps<BM> bitmaps;
    private final MiruPartitionCoord coord;
    private final MiruStreamFactory streamFactory;
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
    private final int partitionRebuildBatchSize;
    private final long partitionRunnableIntervalInMillis;
    private final long partitionMigrationWaitInMillis;
    private final long maxSipClockSkew;
    private final int maxSipReplaySize;

    private final AtomicReference<MiruPartitionStreamGate<BM>> gateRef = new AtomicReference<>();
    private final Object factoryLock = new Object();

    public MiruLocalHostedPartition(
            MiruBitmaps<BM> bitmaps,
            MiruPartitionCoord coord,
            MiruStreamFactory streamFactory,
            MiruActivityWALReader activityWALReader,
            MiruPartitionEventHandler partitionEventHandler,
            ScheduledExecutorService scheduledExecutorService,
            int partitionRebuildBatchSize,
            long partitionBootstrapIntervalInMillis,
            long partitionRunnableIntervalInMillis)
            throws Exception {

        this.bitmaps = bitmaps;
        this.coord = coord;
        this.streamFactory = streamFactory;
        this.activityWALReader = activityWALReader;
        this.partitionEventHandler = partitionEventHandler;
        this.scheduledExecutorService = scheduledExecutorService;
        this.partitionRebuildBatchSize = partitionRebuildBatchSize;
        this.partitionRunnableIntervalInMillis = partitionRunnableIntervalInMillis;
        this.partitionMigrationWaitInMillis = 3000; //TODO config
        this.maxSipClockSkew = TimeUnit.SECONDS.toMillis(10); //TODO config
        this.maxSipReplaySize = 100; //TODO config

        MiruPartitionCoordInfo coordInfo = new MiruPartitionCoordInfo(MiruPartitionState.offline, streamFactory.findBackingStorage(coord));
        MiruPartitionStreamGate<BM> gate = new MiruPartitionStreamGate<BM>(bitmaps, coord, coordInfo, null, 0);
        this.gateRef.set(gate);

        scheduledExecutorService.scheduleWithFixedDelay(
                new BootstrapRunnable(), 0, partitionBootstrapIntervalInMillis, TimeUnit.MILLISECONDS);

        runnables = ImmutableList.<Runnable>of(new ManageIndexRunnable());
        futures = Lists.newArrayListWithCapacity(runnables.size());
    }

    private MiruPartitionStreamGate<BM> open(MiruPartitionStreamGate<BM> gate, MiruPartitionCoordInfo coordInfo) throws Exception {
        synchronized (factoryLock) {
            MiruPartitionState openingState;
            if (gate.info.storage.isMemoryBacked()) {
                if (coordInfo.state == MiruPartitionState.offline) {
                    openingState = MiruPartitionState.offline;
                } else {
                    openingState = MiruPartitionState.bootstrap;
                }
            } else {
                openingState = MiruPartitionState.online;
            }

            Optional<MiruStream<BM>> optionalStream = Optional.absent();
            if (openingState != MiruPartitionState.offline && gate.stream == null) {
                MiruStream<BM> stream = streamFactory.allocate(bitmaps, coord, gate.info.storage);
                optionalStream = Optional.of(stream);
            }
            MiruPartitionStreamGate<BM> opened = new MiruPartitionStreamGate<BM>(bitmaps, coord, coordInfo.copyToState(openingState), optionalStream.orNull(),
                    streamFactory.getSip(coord));
            if (updatePartition(gate, opened)) {
                clearFutures();
                for (Runnable runnable : runnables) {
                    ScheduledFuture<?> future = scheduledExecutorService.scheduleWithFixedDelay(
                            runnable, 0, partitionRunnableIntervalInMillis, TimeUnit.MILLISECONDS);
                    futures.add(future);
                }
                return opened;
            } else {
                return gate;
            }
        }
    }

    public MiruQueryHandle<BM> getQueryHandle() throws Exception {
        MiruPartitionStreamGate<BM> gate = gateRef.get();
        if (!removed.get() && gate.needsHotDeploy()) {
            log.info("Hot deploying for query: {}", coord);
            gate = open(gate, gate.info.copyToState(MiruPartitionState.online));
        }
        return gate.getQueryHandle();
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
                MiruPartitionStreamGate<BM> gate = gateRef.get();
                MiruPartitionCoordInfo coordInfo = gate.info.copyToState(MiruPartitionState.offline);
                MiruPartitionStreamGate<BM> closed = new MiruPartitionStreamGate<>(bitmaps, coord, coordInfo, null, 0);
                if (updatePartition(gate, closed)) {
                    if (gate.stream != null) {
                        streamFactory.close(gate.close());
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
        return gateRef.get().info.state;
    }

    @Override
    public MiruBackingStorage getStorage() {
        return gateRef.get().info.storage;
    }

    @Override
    public MiruTenantId getTenantId() {
        return coord.tenantId;
    }

    @Override
    public void index(Iterator<MiruPartitionedActivity> partitionedActivities) throws Exception {
        // intentionally locking all stream writes for the entire batch to avoid getting a lock for each activity
        MiruPartitionStreamGate gate = gateRef.get();
        if (gate.isOpenForWrites()) {
            //TODO handle return case
            gate.indexInternal(partitionedActivities, MiruPartitionStreamGate.IndexStrategy.ingress);
        } else {
            // keep refreshing in case the rebuild takes a while
            if (gateRef.get().info.state == MiruPartitionState.rebuilding) {
                gate.markForRefresh();
            }
            while (partitionedActivities.hasNext()) {
                MiruPartitionedActivity partitionedActivity = partitionedActivities.next();
                if (partitionedActivity.partitionId.equals(coord.partitionId)) {
                    partitionedActivities.remove();
                }
            }
        }
    }

    @Override
    public void warm() {
        gateRef.get().markForRefresh();
    }

    @Override
    public long sizeInMemory() throws Exception {
        long expiresAfter = sizeInMemoryExpiresAfter.get();
        MiruPartitionStreamGate gate = gateRef.get();
        if (System.currentTimeMillis() > expiresAfter && gate.info.state == MiruPartitionState.online) {
            sizeInMemoryBytes.set(gate.stream != null ? gate.stream.sizeInMemory() : 0);
            sizeInMemoryExpiresAfter.set(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1));
        }
        return sizeInMemoryBytes.get();
    }

    @Override
    public long sizeOnDisk() throws Exception {
        long expiresAfter = sizeOnDiskExpiresAfter.get();
        MiruPartitionStreamGate gate = gateRef.get();
        if (System.currentTimeMillis() > expiresAfter && gate.info.state == MiruPartitionState.online) {
            sizeOnDiskBytes.set(gate.stream != null ? gate.stream.sizeOnDisk() : 0);
            sizeOnDiskExpiresAfter.set(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1));
        }
        return sizeOnDiskBytes.get();
    }

    @Override
    public void setStorage(MiruBackingStorage storage) throws Exception {
        updateStorage(gateRef.get(), storage, true);
    }

    private boolean updateStorage(MiruPartitionStreamGate<BM> gate, MiruBackingStorage destinationStorage, boolean force) throws Exception {
        synchronized (factoryLock) {
            boolean updated = false;
            try (MiruMigrationHandle<BM> handle = gate.getMigrationHandle(partitionMigrationWaitInMillis)) {
                // make sure the gate didn't change while getting the handle, and that it's ready to migrate
                if (gateRef.get() == gate && handle.canMigrateTo(destinationStorage)) {
                    MiruBackingStorage existingStorage = gate.info.storage;
                    if (existingStorage == destinationStorage && !force) {
                        log.warn("Partition at {} ignored request to migrate to same storage {}", coord, destinationStorage);

                    } else if (existingStorage.isMemoryBacked() && destinationStorage.isMemoryBacked()) {
                        MiruStream<BM> fromStream = handle.getStream();
                        if (existingStorage == destinationStorage || !existingStorage.isIdentical(destinationStorage)) {
                            // same memory storage, or non-identical memory storage, triggers a rebuild
                            MiruStream<BM> toStream = streamFactory.allocate(bitmaps, coord, destinationStorage);
                            MiruPartitionStreamGate<BM> migrated = handle.migrated(toStream, Optional.of(destinationStorage),
                                    Optional.of(MiruPartitionState.bootstrap), 0);
                            if (updatePartition(gate, migrated)) {
                                streamFactory.close(fromStream);
                                streamFactory.markStorage(coord, destinationStorage);
                                updated = true;
                            } else {
                                log.warn("Partition at {} failed to migrate to {}", coord, destinationStorage);
                            }
                        } else {
                            // different but identical storage updates without a rebuild
                            MiruPartitionStreamGate<BM> migrated = handle.migrated(fromStream, Optional.of(destinationStorage),
                                    Optional.<MiruPartitionState>absent(), gate.sipTimestamp.get());
                            if (updatePartition(gate, migrated)) {
                                streamFactory.markStorage(coord, destinationStorage);
                                updated = true;
                            } else {
                                log.warn("Partition at {} failed to migrate to {}", coord, destinationStorage);
                            }
                        }
                    } else if (existingStorage.isDiskBacked() && destinationStorage.isMemoryBacked()) {
                        MiruStream<BM> fromStream = handle.getStream();
                        MiruStream<BM> toStream = streamFactory.allocate(bitmaps, coord, destinationStorage);
                        // transitioning to memory, need to bootstrap and rebuild
                        Optional<MiruPartitionState> migrateToState = (gate.info.state == MiruPartitionState.offline)
                                ? Optional.<MiruPartitionState>absent()
                                : Optional.of(MiruPartitionState.bootstrap);
                        MiruPartitionStreamGate<BM> migrated = handle.migrated(toStream, Optional.of(destinationStorage), migrateToState, 0);
                        if (updatePartition(gate, migrated)) {
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

                        MiruStream<BM> fromStream = handle.getStream();
                        MiruStream<BM> toStream;
                        if (destinationStorage == MiruBackingStorage.mem_mapped) {
                            toStream = streamFactory.copyMemMapped(bitmaps, coord, fromStream);
                        } else {
                            toStream = streamFactory.copyToDisk(bitmaps, coord, fromStream);
                        }

                        streamFactory.markSip(coord, gate.sipTimestamp.get());
                        MiruPartitionStreamGate<BM> migrated = handle.migrated(toStream, Optional.of(destinationStorage), Optional.<MiruPartitionState>absent(),
                                gate.sipTimestamp.get());

                        if (updatePartition(gate, migrated)) {
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
                        MiruStream<BM> fromStream = handle.getStream();
                        MiruStream<BM> toStream = streamFactory.allocate(bitmaps, coord, destinationStorage);
                        MiruPartitionStreamGate<BM> migrated = handle.migrated(toStream, Optional.of(destinationStorage), Optional.<MiruPartitionState>absent(),
                                gate.sipTimestamp.get());

                        if (updatePartition(gate, migrated)) {
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

    private boolean updatePartition(MiruPartitionStreamGate<BM> existing, MiruPartitionStreamGate<BM> update) throws Exception {
        if (gateRef.compareAndSet(existing, update)) {
            Optional<Long> refreshTimestamp = Optional.absent();
            if (update.info.state != MiruPartitionState.offline) {
                refreshTimestamp = Optional.of(System.currentTimeMillis());
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
                } catch (Throwable t) {
                    log.error("CheckActive encountered a problem", t);
                }
            } catch (Throwable t) {
                log.error("Bootstrap encountered a problem", t);
            }
        }

        private void refreshTopology() throws Exception {
            long timestamp = gateRef.get().refreshTimestamp.getAndSet(0);
            if (timestamp > 0) {
                MiruPartitionCoordMetrics metrics = new MiruPartitionCoordMetrics(sizeInMemory(), sizeOnDisk());
                partitionEventHandler.refreshTopology(coord, metrics, timestamp);
            }
        }

        private void checkActive() throws Exception {
            if (removed.get()) {
                return;
            }

            MiruPartitionStreamGate<BM> gate = gateRef.get();
            if (partitionEventHandler.isCoordActive(coord)) {
                if (gate.info.state == MiruPartitionState.offline) {
                    open(gate, new MiruPartitionCoordInfo(MiruPartitionState.bootstrap, gate.info.storage));
                }
            } else {
                if (gate.info.state != MiruPartitionState.offline) {
                    close();
                }
            }
        }

    }

    protected class ManageIndexRunnable implements Runnable {

        @Override
        public void run() {
            try {
                MiruPartitionStreamGate<BM> gate = gateRef.get();
                MiruPartitionState state = gate.info.state;
                if (state == MiruPartitionState.offline) {
                    // do nothing
                } else if (state == MiruPartitionState.bootstrap || state == MiruPartitionState.rebuilding) {
                    MiruPartitionStreamGate<BM> rebuilding = gate.copyToState(MiruPartitionState.rebuilding);
                    if (updatePartition(gate, rebuilding)) {
                        try {
                            if (rebuild(rebuilding)) {
                                MiruPartitionStreamGate<BM> online = rebuilding.copyToState(MiruPartitionState.online);
                                updatePartition(rebuilding, online);
                            }
                        } catch (Throwable t) {
                            log.error("Rebuild encountered a problem", t);
                        }
                    }
                } else if (state == MiruPartitionState.online) {
                    try {
                        sip(gate);
                    } catch (Throwable t) {
                        log.error("Sip encountered a problem", t);
                    }
                    try {
                        migrate(gate);
                    } catch (Throwable t) {
                        log.error("Migrate encountered a problem", t);
                    }
                }
            } catch (Throwable t) {
                log.error("ManageIndex encountered a problem", t);
            }
        }

        private boolean rebuild(final MiruPartitionStreamGate gate) throws Exception {
            final List<MiruPartitionedActivity> partitionedActivities = Lists.newLinkedList();
            final AtomicLong rebuildTimestamp = new AtomicLong(gate.rebuildTimestamp.get());
            final AtomicLong sipTimestamp = new AtomicLong(gate.sipTimestamp.get());

            activityWALReader.stream(coord.tenantId, coord.partitionId, gate.rebuildTimestamp.get(),
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
                                gate.indexInternal(partitionedActivities.iterator(), MiruPartitionStreamGate.IndexStrategy.rebuild);
                                gate.rebuildTimestamp.set(rebuildTimestamp.get());
                                gate.sipTimestamp.set(sipTimestamp.get());
                                // indexInternal inherently clears the list by removing elements from the iterator, but just to be safe
                                partitionedActivities.clear();
                            }

                            // stop if the gate has changed
                            return gateRef.get() == gate;
                        }
                    }
            );

            if (!partitionedActivities.isEmpty()) {
                gate.indexInternal(partitionedActivities.iterator(), MiruPartitionStreamGate.IndexStrategy.rebuild);
                gate.rebuildTimestamp.set(rebuildTimestamp.get());
                gate.sipTimestamp.set(sipTimestamp.get());
            }

            return gateRef.get() == gate;
        }

        private boolean sip(final MiruPartitionStreamGate<BM> gate) throws Exception {
            if (!gate.isOpenForWrites()) {
                return false;
            }

            final MiruSipTracker sipTracker = new MiruSipTracker(maxSipReplaySize, maxSipClockSkew, gate.seenLastSip.get());

            long afterTimestamp = gate.sipTimestamp.get();
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

                            // stop if the gate has changed
                            return gateRef.get() == gate;
                        }
                    }
            );
            gate.indexInternal(partitionedActivities.iterator(), MiruPartitionStreamGate.IndexStrategy.sip);

            long suggestedTimestamp = sipTracker.suggestTimestamp(afterTimestamp);
            boolean sipUpdated = gate.sipTimestamp.compareAndSet(afterTimestamp, suggestedTimestamp);
            if (sipUpdated) {
                if (gate.info.storage.isDiskBacked()) {
                    streamFactory.markSip(coord, suggestedTimestamp);
                }
                gate.seenLastSip.compareAndSet(sipTracker.getSeenLastSip(), sipTracker.getSeenThisSip());
            }

            return gateRef.get() == gate;
        }

        private boolean migrate(MiruPartitionStreamGate<BM> gate) throws Exception {
            return gate.canAutoMigrate() && updateStorage(gate, MiruBackingStorage.mem_mapped, false);
        }

    }

    @Override
    public String toString() {
        return "MiruLocalHostedPartition{" +
                "coord=" + coord +
                ", gate=" + gateRef.get() +
                '}';
    }

}
