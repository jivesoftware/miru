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
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaUnvailableException;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActive;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.api.wal.Sip;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.service.NamedThreadFactory;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.service.stream.MiruIndexer;
import com.jivesoftware.os.miru.service.stream.MiruRebuildDirector;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.ArrayList;
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
public class MiruLocalHostedPartition<BM> implements MiruHostedPartition, MiruQueryablePartition<BM> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruBitmaps<BM> bitmaps;
    private final MiruPartitionCoord coord;
    private final MiruContextFactory contextFactory;
    private final MiruWALClient walClient;
    private final MiruPartitionHeartbeatHandler heartbeatHandler;
    private final MiruRebuildDirector rebuildDirector;
    private final AtomicBoolean removed = new AtomicBoolean(false);

    private final Collection<ScheduledFuture<?>> futures;
    private final ScheduledExecutorService scheduledRebuildExecutor;
    private final ScheduledExecutorService scheduledSipExecutor;
    private final ExecutorService rebuildWALExecutors;
    private final ExecutorService sipIndexExecutor;
    private final ExecutorService mergeExecutor;
    private final int rebuildIndexerThreads;
    private final MiruIndexRepairs indexRepairs;
    private final MiruIndexer<BM> indexer;
    private final boolean partitionWakeOnIndex;
    private final int partitionRebuildBatchSize;
    private final int partitionSipBatchSize;
    private final MiruMergeChits mergeChits;
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
        MiruWALClient walClient,
        MiruPartitionHeartbeatHandler heartbeatHandler,
        MiruRebuildDirector rebuildDirector,
        ScheduledExecutorService scheduledBootstrapExecutor,
        ScheduledExecutorService scheduledRebuildExecutor,
        ScheduledExecutorService scheduledSipExecutor,
        ExecutorService rebuildWALExecutors,
        ExecutorService sipIndexExecutor,
        ExecutorService mergeExecutor,
        int rebuildIndexerThreads,
        MiruIndexRepairs indexRepairs,
        MiruIndexer<BM> indexer,
        boolean partitionWakeOnIndex,
        int partitionRebuildBatchSize,
        int partitionSipBatchSize,
        MiruMergeChits mergeChits,
        Timings timings)
        throws Exception {

        this.bitmaps = bitmaps;
        this.coord = coord;
        this.contextFactory = contextFactory;
        this.walClient = walClient;
        this.heartbeatHandler = heartbeatHandler;
        this.rebuildDirector = rebuildDirector;
        this.scheduledRebuildExecutor = scheduledRebuildExecutor;
        this.scheduledSipExecutor = scheduledSipExecutor;
        this.rebuildWALExecutors = rebuildWALExecutors;
        this.sipIndexExecutor = sipIndexExecutor;
        this.mergeExecutor = mergeExecutor;
        this.rebuildIndexerThreads = rebuildIndexerThreads;
        this.indexRepairs = indexRepairs;
        this.indexer = indexer;
        this.partitionWakeOnIndex = partitionWakeOnIndex;
        this.partitionRebuildBatchSize = partitionRebuildBatchSize;
        this.partitionSipBatchSize = partitionSipBatchSize;
        this.mergeChits = mergeChits;
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
        heartbeatHandler.heartbeat(coord, Optional.of(coordInfo), Optional.<Long>absent());
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
                    accessor.refundChits(mergeChits);
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
    public MiruRequestHandle<BM> acquireQueryHandle() throws Exception {
        MiruPartitionAccessor<BM> accessor = accessorRef.get();
        if (!removed.get() && accessor.canHotDeploy()) {
            log.info("Hot deploying for query: {}", coord);
            accessor = open(accessor, accessor.info.copyToState(MiruPartitionState.online));
        }
        heartbeatHandler.heartbeat(coord, Optional.<MiruPartitionCoordInfo>absent(), Optional.of(System.currentTimeMillis()));
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
                MiruPartitionAccessor<BM> existing = accessorRef.get();
                MiruPartitionCoordInfo coordInfo = existing.info.copyToState(MiruPartitionState.offline);
                MiruPartitionAccessor<BM> closed = new MiruPartitionAccessor<>(bitmaps, coord, coordInfo, Optional.<MiruContext<BM>>absent(), indexRepairs,
                    indexer);
                closed = updatePartition(existing, closed);
                if (closed != null) {
                    if (existing.context != null) {
                        Optional<MiruContext<BM>> closedContext = existing.close();
                        if (closedContext.isPresent()) {
                            contextFactory.close(closedContext.get(), existing.info.storage);
                        }
                    }
                    existing.refundChits(mergeChits);
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
            ExecutorService sameThreadExecutor = MoreExecutors.sameThreadExecutor();
            int count = accessor.indexInternal(partitionedActivities, MiruPartitionAccessor.IndexStrategy.ingress, true, mergeChits,
                sameThreadExecutor, sameThreadExecutor);
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
            heartbeatHandler.heartbeat(coord, Optional.<MiruPartitionCoordInfo>absent(), Optional.of(System.currentTimeMillis()));
        }
    }

    @Override
    public void warm() throws Exception {
        heartbeatHandler.heartbeat(coord, Optional.<MiruPartitionCoordInfo>absent(), Optional.of(System.currentTimeMillis()));

        log.inc("warm", 1);
        log.inc("warm", 1, coord.tenantId.toString());
        log.inc("warm>partition>" + coord.partitionId, 1, coord.tenantId.toString());
    }

    @Override
    public boolean setStorage(MiruBackingStorage storage) throws Exception {
        return updateStorage(accessorRef.get(), storage, true);
    }

    private boolean updateStorage(MiruPartitionAccessor<BM> accessor, MiruBackingStorage destinationStorage, boolean force) throws Exception {
        synchronized (factoryLock) {
            boolean updated = false;
            try (MiruMigrationHandle<BM> handle = accessor.getMigrationHandle(timings.partitionMigrationWaitInMillis)) {
                // make sure the accessor didn't change while getting the handle, and that it's ready to migrate
                if (accessorRef.get() == accessor && handle.canMigrateTo(destinationStorage)) {
                    Optional<MiruContext<BM>> optionalFromContext = handle.getContext();
                    MiruBackingStorage existingStorage = accessor.info.storage;
                    if (!optionalFromContext.isPresent()) {
                        log.warn("Partition at {} ignored request to migrate from empty context", coord);

                    } else if (existingStorage == destinationStorage && !force) {
                        log.warn("Partition at {} ignored request to migrate to same storage {}", coord, destinationStorage);
                    } else if (existingStorage == MiruBackingStorage.memory && destinationStorage == MiruBackingStorage.disk) {
                        MiruContext<BM> fromContext = optionalFromContext.get();
                        contextFactory.cleanDisk(coord);

                        MiruContext<BM> toContext;
                        synchronized (fromContext.writeLock) {
                            handle.merge(mergeChits, mergeExecutor);
                            toContext = contextFactory.copy(bitmaps, coord, fromContext, destinationStorage);
                        }

                        MiruPartitionAccessor<BM> migrated = handle.migrated(toContext, Optional.of(destinationStorage),
                            Optional.<MiruPartitionState>absent());

                        migrated = updatePartition(accessor, migrated);
                        if (migrated != null) {
                            contextFactory.close(fromContext, existingStorage);
                            contextFactory.markStorage(coord, destinationStorage);
                            updated = true;
                        } else {
                            log.warn("Partition at {} failed to migrate to {}, attempting to rewind", coord, destinationStorage);
                            contextFactory.close(toContext, destinationStorage);
                            contextFactory.cleanDisk(coord);
                            contextFactory.markStorage(coord, existingStorage);
                        }
                    } else if (existingStorage == MiruBackingStorage.disk && destinationStorage == MiruBackingStorage.memory) {
                        MiruContext<BM> toContext = contextFactory.allocate(bitmaps, coord, destinationStorage);
                        MiruPartitionAccessor<BM> migrated = handle.migrated(toContext, Optional.of(destinationStorage),
                            Optional.of(MiruPartitionState.bootstrap));

                        migrated = updatePartition(accessor, migrated);
                        if (migrated != null) {
                            Optional<MiruContext<BM>> closed = accessor.close();
                            if (closed.isPresent()) {
                                contextFactory.close(closed.get(), existingStorage);
                            }
                            contextFactory.cleanDisk(coord);
                            contextFactory.markStorage(coord, destinationStorage);
                            updated = true;
                        } else {
                            log.warn("Partition at {} failed to migrate to {}, attempting to rewind", coord, destinationStorage);
                            contextFactory.close(toContext, destinationStorage);
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

    private MiruPartitionAccessor<BM> updatePartition(MiruPartitionAccessor<BM> existing, MiruPartitionAccessor<BM> update) throws Exception {
        synchronized (accessorRef) {
            if (accessorRef.get() != existing) {
                return null;
            }

            heartbeatHandler.heartbeat(coord, Optional.of(update.info), Optional.<Long>absent());

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
                    checkActive();
                } catch (MiruSchemaUnvailableException sue) {
                    log.warn("Tenant is active but schema not available for {}", coord.tenantId);
                    log.debug("Tenant is active but schema not available", sue);
                } catch (Throwable t) {
                    log.error("CheckActive encountered a problem for {}", new Object[] { coord }, t);
                }
            } catch (Throwable t) {
                log.error("Bootstrap encountered a problem for {}", new Object[] { coord }, t);
            }
        }

        private void checkActive() throws Exception {
            if (removed.get() || banUnregisteredSchema.get() >= System.currentTimeMillis()) {
                return;
            }

            MiruPartitionAccessor<BM> accessor = accessorRef.get();
            MiruPartitionActive partitionActive = heartbeatHandler.isCoordActive(coord);
            if (partitionActive.active) {
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
                } else if (accessor.context.isPresent() && partitionActive.idle) {
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
                    List<MiruActivityWALStatus> partitionStatus = walClient.getPartitionStatus(coord.tenantId, Collections.singletonList(coord.partitionId));
                    MiruActivityWALStatus status = partitionStatus.get(0);
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
                                            updated.merge(mergeChits, mergeExecutor);
                                        }
                                    }
                                } catch (Throwable t) {
                                    log.error("Rebuild encountered a problem for {}", new Object[] { coord }, t);
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
                log.error("RebuildIndex encountered a problem for {}", new Object[] { coord }, t);
            }
        }

        private boolean rebuild(final MiruPartitionAccessor<BM> accessor) throws Exception {
            final ArrayBlockingQueue<List<MiruPartitionedActivity>> queue = new ArrayBlockingQueue<>(1);
            final AtomicLong rebuildTimestamp = new AtomicLong(accessor.getRebuildTimestamp());
            final AtomicReference<Sip> sip = new AtomicReference<>(accessor.getSip());
            final AtomicBoolean rebuilding = new AtomicBoolean(true);
            final AtomicBoolean endOfWAL = new AtomicBoolean(false);

            log.debug("Starting rebuild at {} for {}", rebuildTimestamp.get(), coord);

            rebuildWALExecutors.submit(new Runnable() {
                @Override
                public void run() {
                    try {

                        MiruWALClient.StreamBatch<MiruWALEntry, MiruWALClient.GetActivityCursor> activity = walClient.getActivity(coord.tenantId,
                            coord.partitionId,
                            new MiruWALClient.GetActivityCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), accessor.getRebuildTimestamp()),
                            partitionRebuildBatchSize);

                        while (rebuilding.get() && accessorRef.get() == accessor && activity != null && !activity.batch.isEmpty()) {
                            List<MiruPartitionedActivity> bs = new ArrayList<>(activity.batch.size());
                            for (MiruWALEntry batch : activity.batch) {
                                bs.add(batch.activity);
                            }
                            tryQueuePut(rebuilding, queue, bs);

                            activity = (activity.cursor != null)
                                ? walClient.getActivity(coord.tenantId, coord.partitionId, activity.cursor, partitionRebuildBatchSize)
                                : null;
                        }

                        // signals end of rebuild
                        log.debug("Signaling end of rebuild for {}", coord);
                        endOfWAL.set(true);
                    } catch (Exception x) {
                        log.error("Failure while rebuilding {}", new Object[] { coord }, x);
                    } finally {
                        rebuilding.set(false);
                    }
                }
            });

            final ExecutorService rebuildIndexExecutor = Executors.newFixedThreadPool(rebuildIndexerThreads,
                new NamedThreadFactory(Thread.currentThread().getThreadGroup(), "rebuild_index_" + coord.tenantId + "_" + coord.partitionId));

            Exception failure = null;
            try {
                List<MiruPartitionedActivity> partitionedActivities;
                int totalIndexed = 0;
                while (true) {
                    partitionedActivities = null;
                    while ((rebuilding.get() || !queue.isEmpty()) && partitionedActivities == null) {
                        partitionedActivities = queue.poll(1, TimeUnit.SECONDS);
                    }
                    if (!rebuilding.get() || partitionedActivities == null) {
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
                    boolean repair = firstRebuild.compareAndSet(true, false);
                    accessor.indexInternal(partitionedActivities.iterator(), MiruPartitionAccessor.IndexStrategy.rebuild, repair, mergeChits,
                        rebuildIndexExecutor, mergeExecutor);
                    accessor.merge(mergeChits, mergeExecutor);
                    accessor.setRebuildTimestamp(rebuildTimestamp.get());
                    accessor.setSip(sip.get());

                    log.stopTimer("rebuild>batchSize-" + partitionRebuildBatchSize);
                    log.inc("rebuild>count>calls", 1);
                    log.inc("rebuild>count>total", count);
                    log.inc("rebuild>count>power>" + FilerIO.chunkPower(count, 0), 1);
                    log.inc("rebuild", count, coord.tenantId.toString());
                    log.inc("rebuild>partition>" + coord.partitionId, count, coord.tenantId.toString());
                }
            } catch (Exception e) {
                log.error("Failure during rebuild index for {}", new Object[] { coord }, e);
                failure = e;
            }

            rebuilding.set(false);
            boolean shutdown = false;
            boolean interrupted = false;
            while (!shutdown) {
                rebuildIndexExecutor.shutdown();
                try {
                    shutdown = rebuildIndexExecutor.awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    log.warn("Ignored interrupt while awaiting shutdown of rebuild index executor for {}", coord);
                    interrupted = true;
                }
            }
            if (interrupted) {
                if (failure != null) {
                    failure = new InterruptedException("Interrupted awaiting rebuild index executor termination after failure: " + failure.getMessage());
                } else {
                    failure = new InterruptedException("Interrupted awaiting rebuild index executor termination");
                }
            }
            if (failure != null) {
                throw failure;
            }

            return endOfWAL.get() && accessorRef.get() == accessor;
        }

        private boolean tryQueuePut(AtomicBoolean rebuilding, ArrayBlockingQueue<List<MiruPartitionedActivity>> queue, List<MiruPartitionedActivity> batch)
            throws InterruptedException {
            boolean success = false;
            while (rebuilding.get() && !success) {
                success = queue.offer(batch, 1, TimeUnit.SECONDS);
            }
            return success;
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
                        if (accessor.canAutoMigrate()) {
                            updateStorage(accessor, MiruBackingStorage.disk, false);
                        }
                    } catch (Throwable t) {
                        log.error("Migrate encountered a problem for {}", new Object[] { coord }, t);
                    }
                    try {
                        if (accessor.isOpenForWrites() && accessor.hasOpenWriters()) {
                            sip(accessor);
                        } else {
                            accessor.refundChits(mergeChits);
                        }
                    } catch (Throwable t) {
                        log.error("Sip encountered a problem for {}", new Object[] { coord }, t);
                    }
                }
            } catch (Throwable t) {
                log.error("SipMigrateIndex encountered a problem for {}", new Object[] { coord }, t);
            }
        }

        private boolean sip(final MiruPartitionAccessor<BM> accessor) throws Exception {

            final MiruSipTracker sipTracker = new MiruSipTracker(maxSipReplaySize, maxSipClockSkew, accessor.seenLastSip.get());

            AtomicReference<Sip> sip = new AtomicReference<>(accessor.getSip());
            final List<MiruPartitionedActivity> partitionedActivities = Lists.newArrayList();

            MiruWALClient.StreamBatch<MiruWALEntry, MiruWALClient.SipActivityCursor> sippedActivity = walClient.sipActivity(coord.tenantId, coord.partitionId,
                new MiruWALClient.SipActivityCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), sip.get().clockTimestamp, sip.get().activityTimestamp),
                partitionSipBatchSize);

            while (accessorRef.get() == accessor && sippedActivity != null && !sippedActivity.batch.isEmpty()) {
                if (Thread.interrupted()) {
                    throw new InterruptedException("Interrupted while streaming sip");
                }

                for (MiruWALEntry e : sippedActivity.batch) {
                    long version = e.activity.activity.isPresent() ? e.activity.activity.get().version : 0; // Smells!
                    TimeAndVersion timeAndVersion = new TimeAndVersion(e.activity.timestamp, version);

                    if (e.activity.type.isBoundaryType() || !sipTracker.wasSeenLastSip(timeAndVersion)) {
                        partitionedActivities.add(e.activity);
                    }
                    sipTracker.addSeenThisSip(timeAndVersion);

                    if (!e.activity.type.isBoundaryType()) {
                        sipTracker.put(e.activity.clockTimestamp, e.activity.timestamp);
                    }

                    if (partitionedActivities.size() > partitionSipBatchSize) {
                        deliver(partitionedActivities, accessor, sipTracker, sip);
                        partitionedActivities.clear();
                    }
                }

                sippedActivity = (sippedActivity.cursor != null)
                    ? walClient.sipActivity(coord.tenantId, coord.partitionId, sippedActivity.cursor, partitionSipBatchSize)
                    : null;
            }

            deliver(partitionedActivities, accessor, sipTracker, sip);

            if (!accessor.hasOpenWriters()) {
                accessor.merge(mergeChits, mergeExecutor);
            }

            return accessorRef.get() == accessor;
        }

        private void deliver(final List<MiruPartitionedActivity> partitionedActivities, final MiruPartitionAccessor<BM> accessor,
            final MiruSipTracker sipTracker, AtomicReference<Sip> sip) throws Exception {
            boolean repair = firstSip.compareAndSet(true, false);
            int initialCount = partitionedActivities.size();
            int count = accessor.indexInternal(partitionedActivities.iterator(), MiruPartitionAccessor.IndexStrategy.sip,
                repair, mergeChits, sipIndexExecutor, mergeExecutor);

            Sip suggestion = sipTracker.suggest(sip.get());
            if (accessor.setSip(suggestion)) {
                sip.set(suggestion);
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
