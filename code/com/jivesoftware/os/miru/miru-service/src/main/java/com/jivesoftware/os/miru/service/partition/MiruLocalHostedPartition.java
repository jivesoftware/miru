package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.jive.utils.health.api.HealthCounter;
import com.jivesoftware.os.jive.utils.health.api.HealthFactory;
import com.jivesoftware.os.jive.utils.health.api.MinMaxHealthCheckConfig;
import com.jivesoftware.os.jive.utils.health.api.MinMaxHealthChecker;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruPartitionActiveTimestamp;
import com.jivesoftware.os.miru.cluster.schema.MiruSchemaUnvailableException;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.service.NamedThreadFactory;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.service.stream.MiruIndexer;
import com.jivesoftware.os.miru.service.stream.MiruRebuildDirector;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader.Sip;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALStatus;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    private final MiruBitmaps<BM> bitmaps;
    private final MiruPartitionCoord coord;
    private final MiruContextFactory contextFactory;
    private final MiruActivityWALReader activityWALReader;
    private final MiruPartitionEventHandler partitionEventHandler;
    private final MiruRebuildDirector rebuildDirector;
    private final AtomicBoolean removed = new AtomicBoolean(false);

    private final Collection<ScheduledFuture<?>> futures;
    private final ScheduledExecutorService scheduledRebuildExecutor;
    private final ScheduledExecutorService scheduledSipExecutor;
    private final ExecutorService rebuildWALExecutors;
    private final ExecutorService sipIndexExecutor;
    private final int rebuildIndexerThreads;
    private final MiruIndexRepairs indexRepairs;
    private final MiruIndexer<BM> indexer;
    private final boolean partitionWakeOnIndex;
    private final int partitionRebuildBatchSize;
    private final int partitionSipBatchSize;
    private final long mergeAfterLiveCount;
    private final long mergeAfterRebuildCount;
    private final Timings timings;
    private final long maxSipClockSkew;
    private final int maxSipReplaySize;

    private final AtomicReference<MiruPartitionAccessor<BM>> accessorRef = new AtomicReference<>();
    private final Object factoryLock = new Object();
    private final AtomicBoolean firstRebuild = new AtomicBoolean(true);
    private final AtomicBoolean firstSip = new AtomicBoolean(true);

    private interface BootstrapCount extends MinMaxHealthCheckConfig {

        @StringDefault("rebuild>pending")
        @Override
        String getName();

        @StringDefault("Number of partitions that need to be rebuilt before service is considered fully online.")
        @Override
        String getDescription();

        @LongDefault(0)
        @Override
        Long getMin();

        @LongDefault(1_000)
        @Override
        Long getMax();
    }

    private static final HealthCounter bootstrapCounter = HealthFactory.getHealthCounter(BootstrapCount.class, MinMaxHealthChecker.FACTORY);

    public MiruLocalHostedPartition(
        MiruBitmaps<BM> bitmaps,
        MiruPartitionCoord coord,
        MiruContextFactory contextFactory,
        MiruActivityWALReader activityWALReader,
        MiruPartitionEventHandler partitionEventHandler,
        MiruRebuildDirector rebuildDirector,
        ScheduledExecutorService scheduledBootstrapExecutor,
        ScheduledExecutorService scheduledRebuildExecutor,
        ScheduledExecutorService scheduledSipExecutor,
        ExecutorService rebuildWALExecutors,
        ExecutorService sipIndexExecutor,
        int rebuildIndexerThreads,
        MiruIndexRepairs indexRepairs,
        MiruIndexer<BM> indexer,
        boolean partitionWakeOnIndex,
        int partitionRebuildBatchSize,
        int partitionSipBatchSize,
        long mergeAfterLiveCount,
        long mergeAfterRebuildCount,
        Timings timings)
        throws Exception {

        this.bitmaps = bitmaps;
        this.coord = coord;
        this.contextFactory = contextFactory;
        this.activityWALReader = activityWALReader;
        this.partitionEventHandler = partitionEventHandler;
        this.rebuildDirector = rebuildDirector;
        this.scheduledRebuildExecutor = scheduledRebuildExecutor;
        this.scheduledSipExecutor = scheduledSipExecutor;
        this.rebuildWALExecutors = rebuildWALExecutors;
        this.sipIndexExecutor = sipIndexExecutor;
        this.rebuildIndexerThreads = rebuildIndexerThreads;
        this.indexRepairs = indexRepairs;
        this.indexer = indexer;
        this.partitionWakeOnIndex = partitionWakeOnIndex;
        this.partitionRebuildBatchSize = partitionRebuildBatchSize;
        this.partitionSipBatchSize = partitionSipBatchSize;
        this.mergeAfterLiveCount = mergeAfterLiveCount;
        this.mergeAfterRebuildCount = mergeAfterRebuildCount;
        this.timings = timings;
        this.maxSipClockSkew = TimeUnit.SECONDS.toMillis(10); //TODO config
        this.maxSipReplaySize = 100; //TODO config
        this.futures = Lists.newCopyOnWriteArrayList(); // rebuild, sip-migrate

        MiruPartitionCoordInfo coordInfo = new MiruPartitionCoordInfo(MiruPartitionState.offline, contextFactory.findBackingStorage(coord));
        MiruPartitionAccessor<BM> accessor = new MiruPartitionAccessor<>(bitmaps,
            coord,
            coordInfo,
            Optional.<MiruContext<BM>>absent(),
            indexRepairs,
            indexer);
        accessor.markForRefresh(Optional.<Long>absent());
        this.accessorRef.set(accessor);
        log.incAtomic("state>" + accessor.info.state.name());
        log.incAtomic("storage>" + accessor.info.storage.name());

        scheduledBootstrapExecutor.scheduleWithFixedDelay(
            new BootstrapRunnable(), 0, timings.partitionBootstrapIntervalInMillis, TimeUnit.MILLISECONDS);

    }

    private MiruPartitionAccessor<BM> open(MiruPartitionAccessor<BM> accessor, MiruPartitionCoordInfo coordInfo) throws Exception {
        synchronized (factoryLock) {
            MiruPartitionState openingState = coordInfo.state;

            Optional<MiruContext<BM>> optionalContext = Optional.absent();
            if (openingState != MiruPartitionState.offline && openingState != MiruPartitionState.bootstrap && !accessor.context.isPresent()) {
                MiruContext<BM> context = contextFactory.allocate(bitmaps, coord, accessor.info.storage);
                optionalContext = Optional.of(context);
            }
            MiruPartitionAccessor<BM> opened = new MiruPartitionAccessor<>(bitmaps, coord, coordInfo.copyToState(openingState), optionalContext,
                indexRepairs, indexer);
            opened = updatePartition(accessor, opened);
            if (opened != null) {
                if (accessor.info.state == MiruPartitionState.offline && openingState != MiruPartitionState.offline) {
                    clearFutures();
                    futures.add(scheduledRebuildExecutor.scheduleWithFixedDelay(new RebuildIndexRunnable(),
                        0, timings.partitionRebuildIntervalInMillis, TimeUnit.MILLISECONDS));
                    futures.add(scheduledSipExecutor.scheduleWithFixedDelay(new SipMigrateIndexRunnable(),
                        0, timings.partitionSipMigrateIntervalInMillis, TimeUnit.MILLISECONDS));
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
        if (!removed.get() && accessor.canHotDeploy()) {
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
                MiruPartitionAccessor<BM> closed = new MiruPartitionAccessor<>(bitmaps, coord, coordInfo, Optional.<MiruContext<BM>>absent(), indexRepairs,
                    indexer);
                closed = updatePartition(accessor, closed);
                if (closed != null) {
                    if (accessor.context != null) {
                        Optional<MiruContext<BM>> closedContext = accessor.close();
                        if (closedContext.isPresent()) {
                            contextFactory.close(closedContext.get(), accessor.info.storage);
                        }
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
            int count = accessor.indexInternal(partitionedActivities, MiruPartitionAccessor.IndexStrategy.ingress, true, Long.MAX_VALUE,
                MoreExecutors.sameThreadExecutor());
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
            accessor.markForRefresh(Optional.of(System.currentTimeMillis()));
        }
    }

    @Override
    public void warm() {
        accessorRef.get().markForRefresh(Optional.of(System.currentTimeMillis()));

        log.inc("warm", 1);
        log.inc("warm", 1, coord.tenantId.toString());
        log.inc("warm>partition>" + coord.partitionId, 1, coord.tenantId.toString());
    }

    @Override
    public void setStorage(MiruBackingStorage storage) throws Exception {
        updateStorage(accessorRef.get(), storage, true);
    }

    private boolean updateStorage(MiruPartitionAccessor<BM> accessor, MiruBackingStorage destinationStorage, boolean force) throws Exception {
        synchronized (factoryLock) {
            boolean updated = false;
            try (MiruMigrationHandle<BM> handle = accessor.getMigrationHandle(timings.partitionMigrationWaitInMillis)) {
                // make sure the accessor didn't change while getting the handle, and that it's ready to migrate
                if (accessorRef.get() == accessor && handle.canMigrateTo(destinationStorage)) {
                    Optional<MiruContext<BM>> fromContext = handle.getContext();
                    MiruBackingStorage existingStorage = accessor.info.storage;
                    if (!fromContext.isPresent()) {
                        log.warn("Partition at {} ignored request to migrate from empty context", coord);

                    } else if (existingStorage == destinationStorage && !force) {
                        log.warn("Partition at {} ignored request to migrate to same storage {}", coord, destinationStorage);
                    } else if (existingStorage == MiruBackingStorage.memory && destinationStorage == MiruBackingStorage.disk) {
                        contextFactory.cleanDisk(coord);

                        MiruContext<BM> toContext = contextFactory.copy(bitmaps, coord, fromContext.get(), destinationStorage);

                        MiruPartitionAccessor<BM> migrated = handle.migrated(toContext, Optional.of(destinationStorage), Optional.<MiruPartitionState>absent());

                        migrated = updatePartition(accessor, migrated);
                        if (migrated != null) {
                            contextFactory.close(fromContext.get(), existingStorage);
                            contextFactory.markStorage(coord, destinationStorage);
                            updated = true;
                        } else {
                            log.warn("Partition at {} failed to migrate to {}, attempting to rewind", coord, destinationStorage);
                            contextFactory.close(toContext, destinationStorage);
                            contextFactory.cleanDisk(coord);
                            contextFactory.markStorage(coord, existingStorage);
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

            update.markForRefresh(Optional.<Long>absent());

            accessorRef.set(update);

            log.decAtomic("state>" + existing.info.state.name());
            log.decAtomic("storage>" + existing.info.storage.name());
            log.incAtomic("state>" + update.info.state.name());
            log.incAtomic("storage>" + update.info.storage.name());
            if (existing.info.state != MiruPartitionState.bootstrap && update.info.state == MiruPartitionState.bootstrap) {
                bootstrapCounter.inc("Total number of partitions that need to be brought online.",
                    "Be patient. Rebalance. Increase number of concurrent rebuilds.");
            } else if (existing.info.state == MiruPartitionState.bootstrap && update.info.state != MiruPartitionState.bootstrap) {
                bootstrapCounter.dec("Total number of partitions that need to be brought online.",
                    "Be patient. Rebalance. Increase number of concurrent rebuilds.");
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
                partitionEventHandler.updateTopology(coord, Optional.of(accessor.info), timestamp);
            }
        }

        private void checkActive() throws Exception {
            if (removed.get() || banUnregisteredSchema.get() >= System.currentTimeMillis()) {
                return;
            }

            MiruPartitionAccessor<BM> accessor = accessorRef.get();
            MiruPartitionActiveTimestamp activeTimestamp = partitionEventHandler.isCoordActive(coord);
            if (activeTimestamp.active) {
                if (accessor.info.state == MiruPartitionState.offline) {
                    if (accessor.info.storage == MiruBackingStorage.memory) {
                        try {
                            open(accessor, accessor.info.copyToState(MiruPartitionState.bootstrap));
                        } catch (MiruPartitionUnavailableException e) {
                            log.warn("CheckActive: Partition is active for tenant {} but no schema is registered, banning for {} ms",
                                coord.tenantId, timings.partitionBanUnregisteredSchemaMillis);
                            banUnregisteredSchema.set(System.currentTimeMillis() + timings.partitionBanUnregisteredSchemaMillis);
                        }
                    } else if (accessor.info.storage == MiruBackingStorage.disk) {
                        if (!removed.get() && accessor.canHotDeploy()) {
                            log.info("Hot deploying for checkActive: {}", coord);
                            open(accessor, accessor.info.copyToState(MiruPartitionState.online));
                        }
                    }
                } else if (accessor.context.isPresent()
                    && activeTimestamp.timestamp < (System.currentTimeMillis() - timings.partitionReleaseContextCacheAfterMillis)) {
                    MiruContext<BM> context = accessor.context.get();
                    contextFactory.releaseCaches(context, accessor.info.storage);
                }
            } else {
                if (accessor.info.state != MiruPartitionState.offline) {
                    close();
                }
            }
        }

    }

    protected class RebuildIndexRunnable implements Runnable {

        private final AtomicLong banUnregisteredSchema = new AtomicLong();

        @Override
        public void run() {
            try {
                MiruPartitionAccessor<BM> accessor = accessorRef.get();
                if (accessor.info.storage != MiruBackingStorage.memory || banUnregisteredSchema.get() >= System.currentTimeMillis()) {
                    return;
                }

                MiruPartitionState state = accessor.info.state;
                if (state == MiruPartitionState.bootstrap || state == MiruPartitionState.rebuilding) {
                    MiruActivityWALStatus status = activityWALReader.getStatus(coord.tenantId, coord.partitionId);
                    Optional<MiruRebuildDirector.Token> token = rebuildDirector.acquire(coord, status.count);
                    if (token.isPresent()) {
                        try {
                            MiruPartitionAccessor<BM> rebuilding;
                            if (state == MiruPartitionState.bootstrap) {
                                rebuilding = open(accessor, accessor.info.copyToState(MiruPartitionState.rebuilding));
                            } else {
                                rebuilding = accessor;
                            }
                            if (rebuilding != null) {
                                try {
                                    if (rebuild(rebuilding)) {
                                        MiruPartitionAccessor<BM> online = rebuilding.copyToState(MiruPartitionState.online);
                                        MiruPartitionAccessor<BM> updated = updatePartition(rebuilding, online);
                                        if (updated != null) {
                                            updated.merge();
                                        }
                                    }
                                } catch (Throwable t) {
                                    log.error("Rebuild encountered a problem", t);
                                }
                            }
                        } catch (MiruPartitionUnavailableException e) {
                            log.warn("Rebuild: Partition is active for tenant {} but no schema is registered, banning for {} ms",
                                coord.tenantId, timings.partitionBanUnregisteredSchemaMillis);
                            banUnregisteredSchema.set(System.currentTimeMillis() + timings.partitionBanUnregisteredSchemaMillis);
                        } finally {
                            rebuildDirector.release(token.get());
                        }
                    } else {
                        log.debug("Skipped rebuild because count={} available={}", status.count, rebuildDirector.available());
                    }
                }
            } catch (Throwable t) {
                log.error("RebuildIndex encountered a problem", t);
            }
        }

        private boolean rebuild(final MiruPartitionAccessor<BM> accessor) throws Exception {
            final ArrayBlockingQueue<List<MiruPartitionedActivity>> queue = new ArrayBlockingQueue<>(1);
            final AtomicLong rebuildTimestamp = new AtomicLong(accessor.getRebuildTimestamp());
            final AtomicReference<Sip> sip = new AtomicReference<>(accessor.getSip());
            final AtomicBoolean rebuilding = new AtomicBoolean(true);
            final AtomicBoolean streaming = new AtomicBoolean(true);

            log.debug("Starting rebuild at {} for {}", rebuildTimestamp.get(), coord);

            final ExecutorService rebuildIndexExecutor = Executors.newFixedThreadPool(rebuildIndexerThreads,
                new NamedThreadFactory(Thread.currentThread().getThreadGroup(), "rebuild_index_" + coord.tenantId + "_" + coord.partitionId));

            rebuildWALExecutors.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        final AtomicReference<List<MiruPartitionedActivity>> batchRef = new AtomicReference<List<MiruPartitionedActivity>>(
                            Lists.<MiruPartitionedActivity>newArrayListWithCapacity(partitionRebuildBatchSize));
                        activityWALReader.stream(coord.tenantId,
                            coord.partitionId,
                            accessor.getRebuildTimestamp(),
                            partitionRebuildBatchSize,
                            timings.partitionRebuildFailureSleepMillis,
                            new MiruActivityWALReader.StreamMiruActivityWAL() {
                                private List<MiruPartitionedActivity> batch = batchRef.get();

                                @Override
                                public boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                                    batch.add(partitionedActivity);
                                    if (batch.size() == partitionRebuildBatchSize) {
                                        log.debug("Delivering batch of size {} for {}", partitionRebuildBatchSize, coord);
                                        if (rebuilding.get()) {
                                            queue.put(batch);
                                        } else {
                                            log.info("Aborting rebuild mid stream from WAL for {}", coord);
                                            return false;
                                        }
                                        batch = Lists.newArrayListWithCapacity(partitionRebuildBatchSize);
                                        batchRef.set(batch);
                                    }

                                    // stop if the rebuild has died, or the accessor has changed
                                    return rebuilding.get() && accessorRef.get() == accessor;
                                }
                            }
                        );

                        List<MiruPartitionedActivity> batch = batchRef.get();
                        if (!batch.isEmpty()) {
                            log.debug("Delivering batch of size {} for {}", batch.size(), coord);
                            if (rebuilding.get()) {
                                queue.put(batch);
                            } else {
                                log.info("Aborting rebuild final stream from WAL for {}", coord);
                                return;
                            }
                        }

                        // signals end of rebuild
                        log.debug("Signaling end of rebuild for {}", coord);
                        if (rebuilding.get()) {
                            queue.put(Collections.<MiruPartitionedActivity>emptyList());
                        } else {
                            log.info("Aborting rebuild end of stream from WAL for {}", coord);
                        }
                    } catch (Exception x) {
                        log.error("Failure while rebuilding {}", new Object[] { coord }, x);
                    } finally {
                        streaming.set(false);
                    }
                }
            });

            List<MiruPartitionedActivity> partitionedActivities;
            int totalIndexed = 0;
            while (true) {
                partitionedActivities = queue.take();
                if (partitionedActivities.isEmpty()) {
                    // end of rebuild
                    log.debug("Ending rebuild for {}", coord);
                    break;
                }

                int count = partitionedActivities.size();
                totalIndexed += count;
                log.debug("Indexing batch of size {} (total {}) for {}", count, totalIndexed, coord);
                for (int i = count - 1; i > -1; i--) {
                    MiruPartitionedActivity partitionedActivity = partitionedActivities.get(i);
                    // only adjust timestamps for activity types
                    if (partitionedActivity.type.isActivityType()) {
                        // rebuild offset is based on the activity timestamp
                        if (partitionedActivity.timestamp > rebuildTimestamp.get()) {
                            rebuildTimestamp.set(partitionedActivity.timestamp);
                        }

                        // activityWAL uses CurrentTimestamper, so column version implies desired sip offset
                        Sip suggestion = new Sip(partitionedActivity.clockTimestamp, partitionedActivity.timestamp);
                        if (suggestion.compareTo(sip.get()) > 0) {
                            sip.set(suggestion);
                        }
                        break;
                    }
                }

                log.startTimer("rebuild>batchSize-" + partitionRebuildBatchSize);
                try {
                    boolean repair = firstRebuild.compareAndSet(true, false);
                    accessor.indexInternal(partitionedActivities.iterator(), MiruPartitionAccessor.IndexStrategy.rebuild, repair, mergeAfterRebuildCount,
                        rebuildIndexExecutor);
                    accessor.setRebuildTimestamp(rebuildTimestamp.get());
                    accessor.setSip(sip.get());
                } catch (Exception e) {
                    log.error("Failure during rebuild index for {}", coord);
                    rebuilding.set(false);
                    // clear the queue to free up rebuild_wal_consumer thread
                    while (streaming.get()) {
                        queue.clear();
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException ie) {
                            log.error("Preventing thread interruption, we have to clear the WAL rebuild thread");
                        }
                    }
                    throw e;
                } finally {
                    log.stopTimer("rebuild>batchSize-" + partitionRebuildBatchSize);
                    log.inc("rebuild>count>calls", 1);
                    log.inc("rebuild>count>total", count);
                    log.inc("rebuild>count>power>" + FilerIO.chunkPower(count, 0), 1);
                    log.inc("rebuild", count, coord.tenantId.toString());
                    log.inc("rebuild>partition>" + coord.partitionId, count, coord.tenantId.toString());
                }
            }

            rebuildIndexExecutor.shutdown();
            try {
                rebuildIndexExecutor.awaitTermination(1, TimeUnit.MINUTES); //TODO expose to config
            } catch (InterruptedException e) {
                log.warn("Rebuild index executor for {} never shut down, threads may leak", coord);
                Thread.interrupted();
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
                log.error("SipIndex encountered a problem", t);
            }
        }

        private boolean sip(final MiruPartitionAccessor<BM> accessor) throws Exception {
            if (!accessor.isOpenForWrites() || !accessor.hasOpenWriters()) {
                return false;
            }

            final MiruSipTracker sipTracker = new MiruSipTracker(maxSipReplaySize, maxSipClockSkew, accessor.seenLastSip.get());

            Sip sip = accessor.getSip();
            final List<MiruPartitionedActivity> partitionedActivities = Lists.newArrayList();
            activityWALReader.streamSip(coord.tenantId,
                coord.partitionId,
                sip,
                partitionSipBatchSize,
                timings.partitionRebuildFailureSleepMillis,
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
                            sipTracker.put(partitionedActivity.clockTimestamp, partitionedActivity.timestamp);
                        }

                        // stop if the accessor has changed
                        return accessorRef.get() == accessor;
                    }
                }
            );

            boolean repair = firstSip.compareAndSet(true, false);
            int initialCount = partitionedActivities.size();
            int count = accessor.indexInternal(partitionedActivities.iterator(), MiruPartitionAccessor.IndexStrategy.sip,
                repair, mergeAfterLiveCount, sipIndexExecutor);

            Sip suggestion = sipTracker.suggest(sip);
            if (accessor.setSip(suggestion)) {
                accessor.seenLastSip.compareAndSet(sipTracker.getSeenLastSip(), sipTracker.getSeenThisSip());
                log.set(ValueType.COUNT, "sipTimestamp>partition>" + coord.partitionId + ">clock",
                    suggestion.clockTimestamp, coord.tenantId.toString());
                log.set(ValueType.COUNT, "sipTimestamp>partition>" + coord.partitionId + ">activity",
                    suggestion.activityTimestamp, coord.tenantId.toString());
            }

            log.inc("sip>count>calls", 1);
            if (count > 0) {
                log.inc("sip>count>total", count);
                log.inc("sip>count>power>" + FilerIO.chunkPower(count, 0), 1);
                log.inc("sip>count", count, coord.tenantId.toString());
                log.inc("sip>partition>" + coord.partitionId, count, coord.tenantId.toString());
            }
            if (initialCount > 0) {
                log.inc("sip>count>skip", (initialCount - count));
            }

            if (!accessor.hasOpenWriters()) {
                accessor.merge();
            }

            return accessorRef.get() == accessor;
        }

        private boolean migrate(MiruPartitionAccessor<BM> accessor) throws Exception {
            return accessor.canAutoMigrate() && updateStorage(accessor, MiruBackingStorage.disk, false);
        }

    }

    @Override
    public String toString() {
        return "MiruLocalHostedPartition{"
            + "coord=" + coord
            + ", accessorRef=" + accessorRef.get()
            + '}';
    }

    public static final class Timings {
        private final long partitionRebuildFailureSleepMillis;
        private final long partitionBootstrapIntervalInMillis;
        private final long partitionRebuildIntervalInMillis;
        private final long partitionSipMigrateIntervalInMillis;
        private final long partitionBanUnregisteredSchemaMillis;
        private final long partitionReleaseContextCacheAfterMillis;
        private final long partitionMigrationWaitInMillis;

        public Timings(long partitionRebuildFailureSleepMillis,
            long partitionBootstrapIntervalInMillis,
            long partitionRebuildIntervalInMillis,
            long partitionSipMigrateIntervalInMillis,
            long partitionBanUnregisteredSchemaMillis,
            long partitionReleaseContextCacheAfterMillis,
            long partitionMigrationWaitInMillis) {
            this.partitionRebuildFailureSleepMillis = partitionRebuildFailureSleepMillis;
            this.partitionBootstrapIntervalInMillis = partitionBootstrapIntervalInMillis;
            this.partitionRebuildIntervalInMillis = partitionRebuildIntervalInMillis;
            this.partitionSipMigrateIntervalInMillis = partitionSipMigrateIntervalInMillis;
            this.partitionBanUnregisteredSchemaMillis = partitionBanUnregisteredSchemaMillis;
            this.partitionReleaseContextCacheAfterMillis = partitionReleaseContextCacheAfterMillis;
            this.partitionMigrationWaitInMillis = partitionMigrationWaitInMillis;
        }
    }

}
