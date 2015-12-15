package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaUnvailableException;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActive;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.service.NamedThreadFactory;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.service.stream.MiruIndexer;
import com.jivesoftware.os.miru.service.stream.MiruRebuildDirector;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.health.api.HealthCounter;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.api.MinMaxHealthCheckConfig;
import com.jivesoftware.os.routing.bird.health.api.MinMaxHealthChecker;
import java.util.ArrayList;
import java.util.Collection;
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
public class MiruLocalHostedPartition<BM extends IBM, IBM, C extends MiruCursor<C, S>, S extends MiruSipCursor<S>>
    implements MiruHostedPartition, MiruQueryablePartition<BM, IBM> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final MiruStats miruStats;
    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final MiruPartitionCoord coord;
    private final AtomicLong expireAfterTimestamp;
    private final MiruContextFactory<S> contextFactory;
    private final MiruSipTrackerFactory<S> sipTrackerFactory;
    private final MiruWALClient<C, S> walClient;
    private final MiruPartitionHeartbeatHandler heartbeatHandler;
    private final MiruRebuildDirector rebuildDirector;
    private final AtomicBoolean removed = new AtomicBoolean(false);

    private final Collection<ScheduledFuture<?>> futures;
    private final ScheduledExecutorService scheduledRebuildExecutor;
    private final ScheduledExecutorService scheduledSipExecutor;
    private final ExecutorService rebuildWALExecutors;
    private final ExecutorService sipIndexExecutor;
    private final ExecutorService persistentMergeExecutor;
    private final ExecutorService transientMergeExecutor;
    private final int rebuildIndexerThreads;
    private final MiruIndexRepairs indexRepairs;
    private final MiruIndexer<BM, IBM> indexer;
    private final boolean partitionAllowNonLatestSchemaInteractions;
    private final int partitionRebuildBatchSize;
    private final int partitionSipBatchSize;
    private final MiruMergeChits persistentMergeChits;
    private final MiruMergeChits transientMergeChits;
    private final Timings timings;

    private final AtomicReference<MiruPartitionAccessor<BM, IBM, C, S>> accessorRef = new AtomicReference<>();
    private final Object factoryLock = new Object();
    private final AtomicBoolean firstRebuild = new AtomicBoolean(true);
    private final AtomicBoolean sipEndOfWAL = new AtomicBoolean(false);
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
        MiruStats miruStats,
        MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        MiruPartitionCoord coord,
        long expireAfterTimestamp,
        MiruContextFactory<S> contextFactory,
        MiruSipTrackerFactory<S> sipTrackerFactory,
        MiruWALClient<C, S> walClient,
        MiruPartitionHeartbeatHandler heartbeatHandler,
        MiruRebuildDirector rebuildDirector,
        ScheduledExecutorService scheduledBootstrapExecutor,
        ScheduledExecutorService scheduledRebuildExecutor,
        ScheduledExecutorService scheduledSipExecutor,
        ExecutorService rebuildWALExecutors,
        ExecutorService sipIndexExecutor,
        ExecutorService persistentMergeExecutor,
        ExecutorService transientMergeExecutor,
        int rebuildIndexerThreads,
        MiruIndexRepairs indexRepairs,
        MiruIndexer<BM, IBM> indexer,
        boolean partitionAllowNonLatestSchemaInteractions,
        int partitionRebuildBatchSize,
        int partitionSipBatchSize,
        MiruMergeChits persistentMergeChits,
        MiruMergeChits transientMergeChits,
        Timings timings)
        throws Exception {

        this.miruStats = miruStats;
        this.bitmaps = bitmaps;
        this.trackError = trackError;
        this.coord = coord;
        this.expireAfterTimestamp = new AtomicLong(expireAfterTimestamp);
        this.contextFactory = contextFactory;
        this.sipTrackerFactory = sipTrackerFactory;
        this.walClient = walClient;
        this.heartbeatHandler = heartbeatHandler;
        this.rebuildDirector = rebuildDirector;
        this.scheduledRebuildExecutor = scheduledRebuildExecutor;
        this.scheduledSipExecutor = scheduledSipExecutor;
        this.rebuildWALExecutors = rebuildWALExecutors;
        this.sipIndexExecutor = sipIndexExecutor;
        this.persistentMergeExecutor = persistentMergeExecutor;
        this.transientMergeExecutor = transientMergeExecutor;
        this.rebuildIndexerThreads = rebuildIndexerThreads;
        this.indexRepairs = indexRepairs;
        this.indexer = indexer;
        this.partitionAllowNonLatestSchemaInteractions = partitionAllowNonLatestSchemaInteractions;
        this.partitionRebuildBatchSize = partitionRebuildBatchSize;
        this.partitionSipBatchSize = partitionSipBatchSize;
        this.persistentMergeChits = persistentMergeChits;
        this.transientMergeChits = transientMergeChits;
        this.timings = timings;
        this.futures = Lists.newCopyOnWriteArrayList(); // rebuild, sip-migrate

        MiruPartitionState initialState = MiruPartitionState.offline;
        MiruBackingStorage initialStorage = contextFactory.findBackingStorage(coord);
        MiruPartitionAccessor<BM, IBM, C, S> accessor = MiruPartitionAccessor.initialize(miruStats,
            bitmaps,
            coord,
            initialState,
            initialStorage == MiruBackingStorage.disk,
            Optional.<MiruContext<BM, IBM, S>>absent(),
            Optional.<MiruContext<BM, IBM, S>>absent(),
            indexRepairs,
            indexer,
            false);

        MiruPartitionCoordInfo coordInfo = new MiruPartitionCoordInfo(initialState, initialStorage);
        heartbeatHandler.heartbeat(coord, Optional.of(coordInfo), Optional.<Long>absent());
        this.accessorRef.set(accessor);
        log.incAtomic("state>" + initialState.name());
        log.incAtomic("storage>" + initialStorage.name());

        scheduledBootstrapExecutor.scheduleWithFixedDelay(
            new BootstrapRunnable(), 0, timings.partitionBootstrapIntervalInMillis, TimeUnit.MILLISECONDS);

    }

    private MiruPartitionAccessor<BM, IBM, C, S> open(MiruPartitionAccessor<BM, IBM, C, S> accessor,
        MiruPartitionState state,
        MiruRebuildDirector.Token rebuildToken,
        StackBuffer stackBuffer) throws Exception {

        Optional<MiruContext<BM, IBM, S>> optionalPersistentContext = Optional.absent();
        Optional<MiruContext<BM, IBM, S>> optionalTransientContext = Optional.absent();


        synchronized (factoryLock) {

            boolean refundPersistentChits = false;
            boolean obsolete = false;
            if (state.isOnline() && accessor.hasPersistentStorage) {
                if (accessor.persistentContext.isPresent()) {
                    optionalPersistentContext = accessor.persistentContext;
                } else {
                    MiruSchema schema = contextFactory.loadPersistentSchema(coord);
                    if (schema == null) {
                        log.warn("Missing schema for persistent storage on {}, marking as obsolete", coord);
                        contextFactory.markObsolete(coord);
                        obsolete = true;
                        schema = contextFactory.lookupLatestSchema(coord.tenantId);
                    }
                    MiruContext<BM, IBM, S> context = contextFactory.allocate(bitmaps, schema, coord, MiruBackingStorage.disk, rebuildToken, stackBuffer);
                    optionalPersistentContext = Optional.of(context);
                    refundPersistentChits = true;
                }
            }

            boolean refundTransientChits = false;
            if (state.isRebuilding()) {
                if (accessor.transientContext.isPresent()) {
                    optionalTransientContext = accessor.transientContext;
                } else {
                    MiruSchema schema = contextFactory.lookupLatestSchema(coord.tenantId);
                    MiruContext<BM, IBM, S> context = contextFactory.allocate(bitmaps, schema, coord, MiruBackingStorage.memory, rebuildToken, stackBuffer);
                    optionalTransientContext = Optional.of(context);
                    refundTransientChits = true;
                }
            }

            MiruPartitionAccessor<BM, IBM, C, S> opened = MiruPartitionAccessor.initialize(miruStats,
                bitmaps,
                coord,
                state,
                accessor.hasPersistentStorage,
                optionalPersistentContext,
                optionalTransientContext,
                indexRepairs,
                indexer,
                obsolete);
            opened = updatePartition(accessor, opened);
            if (opened != null) {
                if (accessor.state == MiruPartitionState.offline && state != MiruPartitionState.offline) {
                    if (refundPersistentChits) {
                        accessor.refundChits(persistentMergeChits);
                    }
                    if (refundTransientChits) {
                        accessor.refundChits(transientMergeChits);
                    }
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

    private MiruPartitionAccessor<BM, IBM, C, S> updatePartition(MiruPartitionAccessor<BM, IBM, C, S> existing,
        MiruPartitionAccessor<BM, IBM, C, S> update) throws Exception {

        synchronized (accessorRef) {
            if (accessorRef.get() != existing) {
                return null;
            }
            MiruBackingStorage updateStorage = update.getBackingStorage();
            MiruPartitionCoordInfo info = new MiruPartitionCoordInfo(update.state, updateStorage);
            heartbeatHandler.heartbeat(coord, Optional.of(info), Optional.<Long>absent());

            accessorRef.set(update);

            MiruBackingStorage existingStorage = existing.getBackingStorage();

            log.decAtomic("state>" + existing.state.name());
            log.decAtomic("storage>" + existingStorage.name());
            log.incAtomic("state>" + update.state.name());
            log.incAtomic("storage>" + updateStorage.name());
            if (existing.state != MiruPartitionState.bootstrap && update.state == MiruPartitionState.bootstrap) {
                bootstrapCounter.inc("Total number of partitions that need to be brought online.",
                    "Be patient. Rebalance. Increase number of concurrent rebuilds.");
            } else if (existing.state == MiruPartitionState.bootstrap && update.state != MiruPartitionState.bootstrap) {
                bootstrapCounter.dec("Total number of partitions that need to be brought online.",
                    "Be patient. Rebalance. Increase number of concurrent rebuilds.");
            }

            log.info("Partition is now {}/{} for {}", update.state, updateStorage, coord);
            return update;
        }
    }

    @Override
    public MiruRequestHandle<BM, IBM, S> acquireQueryHandle(StackBuffer stackBuffer) throws Exception {
        heartbeatHandler.heartbeat(coord, Optional.<MiruPartitionCoordInfo>absent(), Optional.of(System.currentTimeMillis()));

        if (removed.get()) {
            throw new MiruPartitionUnavailableException("Partition has been removed");
        }
        if (!sipEndOfWAL.get()) {
            throw new MiruPartitionUnavailableException("Partition needs to catch up");
        }

        MiruPartitionAccessor<BM, IBM, C, S> accessor = accessorRef.get();
        if (!partitionAllowNonLatestSchemaInteractions && accessor.persistentContext.isPresent()) {
            MiruSchema latestSchema = contextFactory.lookupLatestSchema(coord.tenantId);
            if (!MiruSchema.checkEquals(accessor.persistentContext.get().schema, latestSchema)) {
                throw new MiruPartitionUnavailableException("Partition is outdated");
            }
        }
        if (accessor.canHotDeploy()) {
            log.info("Hot deploying for query: {}", coord);
            accessor = open(accessor, MiruPartitionState.online, null, stackBuffer);
        }
        return accessor.getRequestHandle(trackError);
    }

    @Override
    public MiruRequestHandle<BM, IBM, S> inspectRequestHandle() throws Exception {
        if (removed.get()) {
            throw new MiruPartitionUnavailableException("Partition has been removed");
        }

        MiruPartitionAccessor<BM, IBM, C, S> accessor = accessorRef.get();
        return accessor.getRequestHandle(trackError);
    }

    @Override
    public boolean isAvailable() {
        return !removed.get() && sipEndOfWAL.get();
    }

    @Override
    public void remove() throws Exception {
        log.info("Removing partition by request: {}", coord);
        removed.set(true);
        close();
    }

    private boolean close() throws Exception {
        try {
            synchronized (factoryLock) {
                MiruPartitionAccessor<BM, IBM, C, S> existing = accessorRef.get();
                MiruPartitionAccessor<BM, IBM, C, S> closed = MiruPartitionAccessor.initialize(miruStats,
                    bitmaps,
                    coord,
                    MiruPartitionState.offline,
                    existing.hasPersistentStorage,
                    Optional.<MiruContext<BM, IBM, S>>absent(),
                    Optional.<MiruContext<BM, IBM, S>>absent(),
                    indexRepairs,
                    indexer,
                    false);
                closed = updatePartition(existing, closed);
                if (closed != null) {
                    existing.close(contextFactory, rebuildDirector);
                    existing.refundChits(persistentMergeChits);
                    existing.refundChits(transientMergeChits);
                    clearFutures();
                    return true;
                } else {
                    return false;
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

    public boolean isExpired() {
        long expire = expireAfterTimestamp.get();
        return expire > 0 && System.currentTimeMillis() > expire;
    }

    public boolean needsToMigrate() {
        return accessorRef.get().state == MiruPartitionState.online && accessorRef.get().transientContext.isPresent();
    }

    public void cancelExpiration() {
        expireAfterTimestamp.set(-1);
    }

    @Override
    public MiruPartitionId getPartitionId() {
        return coord.partitionId;
    }

    @Override
    public MiruPartitionState getState() {
        return accessorRef.get().state;
    }

    @Override
    public MiruBackingStorage getStorage() {
        return accessorRef.get().getBackingStorage();
    }

    @Override
    public MiruTenantId getTenantId() {
        return coord.tenantId;
    }

    @Override
    public void index(Iterator<MiruPartitionedActivity> partitionedActivities) throws Exception {
        // intentionally locking all stream writes for the entire batch to avoid getting a lock for each activity
        MiruPartitionAccessor<BM, IBM, C, S> accessor = accessorRef.get();
        if (accessor.isOpenForWrites()) {
            ExecutorService sameThreadExecutor = MoreExecutors.sameThreadExecutor();
            StackBuffer stackBuffer = new StackBuffer();

            Optional<MiruContext<BM, IBM, S>> context;
            MiruMergeChits mergeChits;
            if (accessor.persistentContext.isPresent()) {
                context = accessor.persistentContext;
                mergeChits = persistentMergeChits;
            } else {
                context = accessor.transientContext;
                mergeChits = transientMergeChits;
            }

            int count = accessor.indexInternal(context,
                partitionedActivities,
                MiruPartitionAccessor.IndexStrategy.ingress,
                false,
                mergeChits,
                sameThreadExecutor,
                sameThreadExecutor,
                trackError,
                stackBuffer);
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
    }

    @Override
    public void warm() throws Exception {
        heartbeatHandler.heartbeat(coord, Optional.<MiruPartitionCoordInfo>absent(), Optional.of(System.currentTimeMillis()));
        log.inc("warm", 1);
        log.inc("warm", 1, coord.tenantId.toString());
        log.inc("warm>partition>" + coord.partitionId, 1, coord.tenantId.toString());
    }

    @Override
    public boolean rebuild() throws Exception {
        return rebuild(accessorRef.get());
    }

    private boolean rebuild(MiruPartitionAccessor<BM, IBM, C, S> accessor) throws Exception {
        synchronized (factoryLock) {
            boolean updated = false;
            try (MiruMigrationHandle<BM, IBM, C, S> handle = accessor.getMigrationHandle(timings.partitionMigrationWaitInMillis)) {
                // make sure the accessor didn't change while getting the handle, and that it's ready to migrate
                if (accessorRef.get() == accessor) {
                    if (accessor.transientContext.isPresent()) {
                        handle.closeTransientContext(contextFactory, rebuildDirector);
                    }
                }

                Optional<MiruContext<BM, IBM, S>> newPersistentContext = accessor.persistentContext;
                Optional<MiruContext<BM, IBM, S>> newTransientContext = Optional.absent();
                MiruPartitionState newState = newPersistentContext.isPresent() ? MiruPartitionState.obsolete : MiruPartitionState.bootstrap;
                Optional<Boolean> newHasPersistentStorage = Optional.absent();

                MiruPartitionAccessor<BM, IBM, C, S> migrated = handle.migrated(newPersistentContext,
                    newTransientContext,
                    Optional.of(newState),
                    newHasPersistentStorage);

                if (migrated != null) {
                    updated = updatePartition(accessor, migrated) != null;
                    if (newState == MiruPartitionState.obsolete) {
                        contextFactory.markObsolete(coord);
                    }
                }
            }
            return updated;
        }
    }

    private MiruPartitionAccessor<BM, IBM, C, S> transitionStorage(MiruPartitionAccessor<BM, IBM, C, S> accessor,
        StackBuffer stackBuffer) throws Exception {
        synchronized (factoryLock) {
            MiruPartitionAccessor<BM, IBM, C, S> updated = accessor;
            try (MiruMigrationHandle<BM, IBM, C, S> handle = accessor.getMigrationHandle(timings.partitionMigrationWaitInMillis)) {
                // make sure the accessor didn't change while getting the handle, and that it's ready to migrate
                if (accessorRef.get() == accessor && accessor.transientContext.isPresent()) {

                    handle.closePersistentContext(contextFactory, rebuildDirector);
                    contextFactory.cleanDisk(coord);

                    MiruContext<BM, IBM, S> toContext;
                    MiruContext<BM, IBM, S> fromContext = accessor.transientContext.get();
                    synchronized (fromContext.writeLock) {
                        handle.merge(transientMergeExecutor, accessor.transientContext, transientMergeChits, trackError);
                        toContext = contextFactory.copy(bitmaps, fromContext.schema, coord, fromContext, MiruBackingStorage.disk, stackBuffer);
                        contextFactory.saveSchema(coord, fromContext.schema);
                    }

                    Optional<MiruContext<BM, IBM, S>> newPersistentContext = Optional.of(toContext);
                    Optional<MiruContext<BM, IBM, S>> newTransientContext = Optional.absent();
                    Optional<MiruPartitionState> newState = Optional.of(MiruPartitionState.online);
                    Optional<Boolean> newHasPersistentStorage = Optional.of(true);

                    MiruPartitionAccessor<BM, IBM, C, S> migrated = handle.migrated(newPersistentContext,
                        newTransientContext,
                        newState,
                        newHasPersistentStorage);

                    if (migrated != null) {
                        migrated = updatePartition(accessor, migrated);
                        if (migrated != null) {
                            log.info("Partition at {} has transitioned to persistent storage", coord);
                            contextFactory.close(fromContext, rebuildDirector);
                            updated = migrated;
                        } else {
                            log.warn("Partition at {} failed to migrate to {}, attempting to rewind", coord, MiruBackingStorage.disk);
                            contextFactory.close(toContext, rebuildDirector);
                            contextFactory.cleanDisk(coord);
                        }
                    }
                }
            }
            return updated;
        }
    }

    protected class BootstrapRunnable implements Runnable {

        private final AtomicLong banUnregisteredSchema = new AtomicLong();

        @Override
        public void run() {
            try {
                try {
                    StackBuffer stackBuffer = new StackBuffer();
                    checkActive(stackBuffer);
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

        private void checkActive(StackBuffer stackBuffer) throws Exception {
            if (removed.get() || banUnregisteredSchema.get() >= System.currentTimeMillis()) {
                return;
            }

            MiruPartitionAccessor<BM, IBM, C, S> accessor = accessorRef.get();
            MiruPartitionActive partitionActive = heartbeatHandler.getPartitionActive(coord);
            if (partitionActive.destroyAfterTimestamp > 0 && System.currentTimeMillis() > partitionActive.destroyAfterTimestamp) {
                if (accessor.state != MiruPartitionState.offline) {
                    log.info("Taking partition offline because it is marked for destruction: {}", coord);
                    close();
                    accessor = accessorRef.get();
                }
                if (accessor.state == MiruPartitionState.offline && accessor.hasPersistentStorage) {
                    synchronized (factoryLock) {
                        log.info("Cleaning disk for partition because it is marked for destruction: {}", coord);

                        MiruPartitionAccessor<BM, IBM, C, S> existing = accessorRef.get();

                        MiruPartitionAccessor<BM, IBM, C, S> cleaned = MiruPartitionAccessor.initialize(miruStats,
                            bitmaps,
                            coord,
                            MiruPartitionState.offline,
                            false,
                            Optional.<MiruContext<BM, IBM, S>>absent(),
                            Optional.<MiruContext<BM, IBM, S>>absent(),
                            indexRepairs,
                            indexer,
                            false);
                        cleaned = updatePartition(existing, cleaned);
                        if (cleaned != null) {
                            existing.close(contextFactory, rebuildDirector);
                            existing.refundChits(persistentMergeChits);
                            existing.refundChits(transientMergeChits);
                            clearFutures();
                            contextFactory.cleanDisk(coord);
                        } else {
                            log.warn("Failed to clean disk because accessor changed for partition: {}", coord);
                        }
                    }
                }
            } else if (partitionActive.activeUntilTimestamp > System.currentTimeMillis()) {
                if (accessor.state == MiruPartitionState.offline) {
                    if (accessor.hasPersistentStorage) {
                        if (!removed.get() && accessor.canHotDeploy()) {
                            log.info("Hot deploying for checkActive: {}", coord);
                            open(accessor, MiruPartitionState.online, null, stackBuffer);
                        }
                    } else {
                        try {
                            open(accessor, MiruPartitionState.bootstrap, null, stackBuffer);
                        } catch (MiruPartitionUnavailableException e) {
                            log.warn("CheckActive: Partition is active for tenant {} but no schema is registered, banning for {} ms",
                                coord.tenantId, timings.partitionBanUnregisteredSchemaMillis);
                            banUnregisteredSchema.set(System.currentTimeMillis() + timings.partitionBanUnregisteredSchemaMillis);
                        }
                    }
                } else if (accessor.persistentContext.isPresent() && System.currentTimeMillis() > partitionActive.idleAfterTimestamp) {
                    contextFactory.releaseCaches(accessor.persistentContext.get());
                }
            } else if (accessor.state != MiruPartitionState.offline) {
                if (accessor.transientContext.isPresent()) {
                    log.info("Partition {} is idle but still has a transient context, closure will be deferred", coord);
                } else {
                    close();
                }
            }
        }

    }

    protected class RebuildIndexRunnable implements Runnable {

        private final AtomicLong banUnregisteredSchema = new AtomicLong();

        @Override
        public void run() {
            StackBuffer stackBuffer = new StackBuffer();
            try {
                MiruPartitionAccessor<BM, IBM, C, S> accessor = accessorRef.get();
                if (banUnregisteredSchema.get() >= System.currentTimeMillis()) {
                    return;
                }

                MiruPartitionState state = accessor.state;
                if (state.isRebuildable() || state.isRebuilding()) {
                    long count = estimateActivityCount();
                    Optional<MiruRebuildDirector.Token> token = accessor.getRebuildToken();
                    if (!token.isPresent()) {
                        token = rebuildDirector.acquire(coord, count);
                    }
                    if (token.isPresent()) {
                        try {
                            if (state.isRebuildable()) {
                                MiruPartitionState desiredState = state.transitionToRebuildingState();
                                accessor = open(accessor, desiredState, token.get(), stackBuffer);
                                if (accessor.state != desiredState) {
                                    log.warn("Failed to transition to {} for {}", desiredState, coord);
                                    accessor = null;
                                }
                            }
                            if (accessor != null) {
                                try {
                                    if (!accessor.transientContext.isPresent()) {
                                        log.error("Attempted rebuild without a transientContext for {}", coord);
                                    } else if (accessor.transientContext.get().isCorrupt()) {
                                        if (close()) {
                                            log.warn("Stopped rebuild due to corruption for {}", coord);
                                        } else {
                                            log.error("Failed to stop rebuild after corruption for {}", coord);
                                        }
                                    } else if (rebuild(accessor, stackBuffer)) {
                                        MiruPartitionAccessor<BM, IBM, C, S> online = accessor.copyToState(MiruPartitionState.online);
                                        accessor = updatePartition(accessor, online);
                                        if (accessor != null) {
                                            accessor.merge(transientMergeExecutor, accessor.transientContext, transientMergeChits, trackError);
                                            trackError.reset();
                                        }
                                    } else {
                                        log.error("Rebuild did not finish for {} isAccessor={}", coord, (accessor == accessorRef.get()));
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
                            if (accessor == null || !accessor.getRebuildToken().isPresent()) {
                                rebuildDirector.release(token.get());
                            }
                        }
                    } else {
                        log.debug("Skipped rebuild because count={} available={}", count, rebuildDirector.available());
                    }
                }
            } catch (MiruSchemaUnvailableException e) {
                log.warn("Skipped rebuild because schema is unavailable for {}", coord);
            } catch (Throwable t) {
                log.error("RebuildIndex encountered a problem for {}", new Object[] { coord }, t);
            }
        }

        private final AtomicLong lastActivityCount = new AtomicLong(-1);
        private final AtomicLong lastActivityCountTimestamp = new AtomicLong(-1);

        private long estimateActivityCount() throws Exception {
            long count = lastActivityCount.get();
            long nextEstimateAfterTimestamp = lastActivityCountTimestamp.get() + timings.partitionRebuildEstimateActivityCountIntervalInMillis;
            if (count < 0 || System.currentTimeMillis() > nextEstimateAfterTimestamp) {
                MiruActivityWALStatus status = walClient.getActivityWALStatusForTenant(coord.tenantId, coord.partitionId);
                count = 0;
                for (MiruActivityWALStatus.WriterCount writerCount : status.counts) {
                    count += writerCount.count;
                }
                lastActivityCount.set(count);
                lastActivityCountTimestamp.set(System.currentTimeMillis());
            }
            return count;
        }

        private boolean rebuild(final MiruPartitionAccessor<BM, IBM, C, S> accessor, StackBuffer stackBuffer) throws Exception {
            final ArrayBlockingQueue<MiruWALClient.StreamBatch<MiruWALEntry, C>> queue = new ArrayBlockingQueue<>(1);
            final AtomicReference<C> cursor = new AtomicReference<>(accessor.getRebuildCursor());
            final AtomicBoolean rebuilding = new AtomicBoolean(true);
            final AtomicBoolean endOfWAL = new AtomicBoolean(false);

            log.debug("Starting rebuild at {} for {}", cursor.get(), coord);

            rebuildWALExecutors.submit(() -> {
                try {

                    MiruWALClient.StreamBatch<MiruWALEntry, C> streamBatch = walClient.getActivity(coord.tenantId,
                        coord.partitionId,
                        cursor.get(),
                        partitionRebuildBatchSize);

                    while (rebuilding.get() && accessorRef.get() == accessor && streamBatch != null) {
                        tryQueuePut(rebuilding, queue, streamBatch);
                        if (streamBatch.activities.isEmpty()) {
                            break;
                        }
                        streamBatch = (streamBatch.cursor != null)
                            ? walClient.getActivity(coord.tenantId, coord.partitionId, streamBatch.cursor, partitionRebuildBatchSize)
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
            });

            final ExecutorService rebuildIndexExecutor = Executors.newFixedThreadPool(rebuildIndexerThreads,
                new NamedThreadFactory(Thread.currentThread().getThreadGroup(), "rebuild_index_" + coord.tenantId + "_" + coord.partitionId));

            Exception failure = null;
            try {
                int totalIndexed = 0;
                while (true) {
                    MiruWALClient.StreamBatch<MiruWALEntry, C> streamBatch = null;
                    List<MiruPartitionedActivity> partitionedActivities = null;
                    C nextCursor = null;
                    while ((rebuilding.get() || !queue.isEmpty()) && streamBatch == null) {
                        streamBatch = queue.poll(1, TimeUnit.SECONDS);
                    }

                    if (streamBatch != null) {
                        partitionedActivities = new ArrayList<>(streamBatch.activities.size());
                        for (MiruWALEntry batch : streamBatch.activities) {
                            partitionedActivities.add(batch.activity);
                        }
                        nextCursor = streamBatch.cursor;
                    } else {
                        // end of rebuild
                        log.debug("Ending rebuild for {}", coord);
                        break;
                    }

                    int count = partitionedActivities.size();
                    totalIndexed += count;

                    log.debug("Indexing batch of size {} (total {}) for {}", count, totalIndexed, coord);
                    log.startTimer("rebuild>batchSize-" + partitionRebuildBatchSize);
                    boolean repair = firstRebuild.compareAndSet(true, false);
                    accessor.indexInternal(accessor.transientContext,
                        partitionedActivities.iterator(),
                        MiruPartitionAccessor.IndexStrategy.rebuild,
                        repair,
                        transientMergeChits,
                        rebuildIndexExecutor,
                        transientMergeExecutor,
                        trackError,
                        stackBuffer);
                    //accessor.merge(transientMergeExecutor, accessor.transientContext, transientMergeChits, trackError);
                    accessor.setRebuildCursor(nextCursor);
                    if (nextCursor.getSipCursor() != null) {
                        accessor.setSip(accessor.transientContext, nextCursor.getSipCursor(), stackBuffer);
                    }

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

        private boolean tryQueuePut(AtomicBoolean rebuilding,
            ArrayBlockingQueue<MiruWALClient.StreamBatch<MiruWALEntry, C>> queue,
            MiruWALClient.StreamBatch<MiruWALEntry, C> batch)
            throws InterruptedException {
            boolean success = false;
            while (rebuilding.get() && !success) {
                success = queue.offer(batch, 1, TimeUnit.SECONDS);
            }
            return success;
        }
    }

    protected class SipMigrateIndexRunnable implements Runnable {

        private final AtomicBoolean checkedObsolete = new AtomicBoolean(false);

        private void checkObsolete(MiruPartitionAccessor<BM, IBM, C, S> accessor) throws Exception {
            if (accessor.transientContext.isPresent()) {
                return;
            }

            if (checkedObsolete.compareAndSet(false, true)) {
                if (contextFactory.checkObsolete(coord)) {
                    log.warn("Found obsolete context for {}", coord);
                    accessor.markObsolete();
                    return;
                }
            }

            if (accessor.persistentContext.isPresent() && !accessor.isObsolete()) {
                MiruSchema latestSchema = contextFactory.lookupLatestSchema(coord.tenantId);
                MiruContext<BM, IBM, S> context = accessor.persistentContext.get();
                if (!MiruSchema.checkEquals(latestSchema, context.schema)) {
                    log.warn("Found obsolete schema for {}: {} {} vs {} {}",
                        coord, latestSchema.getName(), latestSchema.getVersion(), context.schema.getName(), context.schema.getVersion());
                    accessor.markObsolete();
                }
            }
        }

        @Override
        public void run() {
            StackBuffer stackBuffer = new StackBuffer();
            try {
                MiruPartitionAccessor<BM, IBM, C, S> accessor = accessorRef.get();
                MiruPartitionState state = accessor.state;
                if (state.isOnline()) {
                    checkObsolete(accessor);

                    boolean forceRebuild = false;
                    if (!accessor.persistentContext.isPresent() && !accessor.transientContext.isPresent()) {
                        log.info("Forcing rebuild because context is missing for {}", coord);
                        forceRebuild = true;
                    } else if (accessor.isCorrupt()) {
                        log.info("Forcing rebuild because context is corrupt for {}", coord);
                        forceRebuild = true;
                    } else if (!accessor.transientContext.isPresent() && accessor.isObsolete() && accessor.state != MiruPartitionState.obsolete) {
                        log.info("Forcing rebuild because context is obsolete for {}", coord);
                        forceRebuild = true;
                    }

                    if (forceRebuild) {
                        rebuild(accessor);
                        return;
                    }

                    try {
                        if (accessor.canAutoMigrate()) {
                            accessor = transitionStorage(accessor, stackBuffer);
                        }
                    } catch (Throwable t) {
                        log.error("Migrate encountered a problem for {}", new Object[] { coord }, t);
                    }
                    try {
                        if (accessor.isOpenForWrites() && accessor.hasOpenWriters()) {
                            if (!partitionAllowNonLatestSchemaInteractions && accessor.persistentContext.isPresent()) {
                                MiruSchema latestSchema = contextFactory.lookupLatestSchema(coord.tenantId);
                                if (!MiruSchema.checkEquals(accessor.persistentContext.get().schema, latestSchema)) {
                                    sipEndOfWAL.set(false);
                                    return;
                                }
                            }

                            if (sip(accessor, stackBuffer)) {
                                sipEndOfWAL.set(true);
                            }
                        } else {
                            accessor.refundChits(persistentMergeChits);
                        }
                    } catch (Throwable t) {
                        log.error("Sip encountered a problem for {}", new Object[] { coord }, t);
                    }
                }
            } catch (Throwable t) {
                log.error("SipMigrateIndex encountered a problem for {}", new Object[] { coord }, t);
            }
        }

        private boolean sip(final MiruPartitionAccessor<BM, IBM, C, S> accessor, StackBuffer stackBuffer) throws Exception {

            final MiruSipTracker<S> sipTracker = sipTrackerFactory.create(accessor.seenLastSip.get());

            S sipCursor = accessor.getSipCursor(stackBuffer).orNull();
            boolean first = firstSip.get();

            MiruWALClient.StreamBatch<MiruWALEntry, S> sippedActivity = walClient.sipActivity(coord.tenantId,
                coord.partitionId,
                sipCursor,
                sipTracker.getSeenLastSip(),
                partitionSipBatchSize);

            boolean sippedEndOfWAL = false;

            while (accessorRef.get() == accessor && sippedActivity != null) {
                if (Thread.interrupted()) {
                    throw new InterruptedException("Interrupted while streaming sip");
                }

                if (sippedActivity.suppressed != null) {
                    for (TimeAndVersion timeAndVersion : sippedActivity.suppressed) {
                        sipTracker.addSeenThisSip(timeAndVersion);
                    }
                }

                List<MiruPartitionedActivity> partitionedActivities = Lists.newArrayListWithCapacity(sippedActivity.activities.size());
                for (MiruWALEntry e : sippedActivity.activities) {
                    long version = e.activity.activity.isPresent() ? e.activity.activity.get().version : 0; // Smells!
                    TimeAndVersion timeAndVersion = new TimeAndVersion(e.activity.timestamp, version);

                    if (!sipTracker.wasSeenLastSip(timeAndVersion)) {
                        partitionedActivities.add(e.activity);
                    }
                    sipTracker.addSeenThisSip(timeAndVersion);
                    sipTracker.track(e.activity);
                }

                S lastCursor = sipCursor;
                sipCursor = deliver(partitionedActivities, accessor, sipTracker, sipCursor, sippedActivity.cursor, stackBuffer);
                partitionedActivities.clear();

                if (sippedActivity.cursor != null && sippedActivity.cursor.endOfStream()) {
                    log.info("Sipped to end of stream for {}", coord);
                    long threshold = first ? 0 : timings.partitionSipNotifyEndOfStreamMillis;
                    sippedEndOfWAL = true;
                    accessor.notifyEndOfStream(threshold);
                    break;
                } else if (sipCursor == null) {
                    log.warn("No cursor for {}", coord);
                    sippedEndOfWAL = true;
                    break;
                } else if (sipCursor.equals(lastCursor)) {
                    log.warn("Sipped same cursor for {}", coord);
                    sippedEndOfWAL = true;
                    break;
                } else if (sippedActivity.endOfWAL) {
                    log.debug("Sipped to end of WAL for {}", coord);
                    sippedEndOfWAL = true;
                    break;
                }

                sippedActivity = walClient.sipActivity(coord.tenantId, coord.partitionId, sipCursor, sipTracker.getSeenThisSip(), partitionSipBatchSize);
            }

            if (!accessor.hasOpenWriters()) {
                accessor.merge(persistentMergeExecutor, accessor.persistentContext, persistentMergeChits, trackError);
            }

            return sippedEndOfWAL;
        }

        private S deliver(final List<MiruPartitionedActivity> partitionedActivities, final MiruPartitionAccessor<BM, IBM, C, S> accessor,
            final MiruSipTracker<S> sipTracker, S sipCursor, S nextSipCursor, StackBuffer stackBuffer) throws Exception {
            boolean repair = firstSip.compareAndSet(true, false);
            int initialCount = partitionedActivities.size();
            int count = accessor.indexInternal(
                accessor.persistentContext,
                partitionedActivities.iterator(),
                MiruPartitionAccessor.IndexStrategy.sip,
                repair,
                persistentMergeChits,
                sipIndexExecutor,
                persistentMergeExecutor,
                trackError,
                stackBuffer);

            S suggestion = sipTracker.suggest(sipCursor, nextSipCursor);
            if (suggestion != null && accessor.setSip(accessor.persistentContext, suggestion, stackBuffer)) {
                accessor.seenLastSip.compareAndSet(sipTracker.getSeenLastSip(), sipTracker.getSeenThisSip());
                sipTracker.metrics(coord, suggestion);
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
            return suggestion;
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

        private final long partitionBootstrapIntervalInMillis;
        private final long partitionRebuildIntervalInMillis;
        private final long partitionSipMigrateIntervalInMillis;
        private final long partitionBanUnregisteredSchemaMillis;
        private final long partitionMigrationWaitInMillis;
        private final long partitionSipNotifyEndOfStreamMillis;
        private final long partitionRebuildEstimateActivityCountIntervalInMillis;

        public Timings(long partitionBootstrapIntervalInMillis,
            long partitionRebuildIntervalInMillis,
            long partitionSipMigrateIntervalInMillis,
            long partitionBanUnregisteredSchemaMillis,
            long partitionMigrationWaitInMillis,
            long partitionSipNotifyEndOfStreamMillis,
            long partitionRebuildEstimateActivityCountIntervalInMillis) {
            this.partitionBootstrapIntervalInMillis = partitionBootstrapIntervalInMillis;
            this.partitionRebuildIntervalInMillis = partitionRebuildIntervalInMillis;
            this.partitionSipMigrateIntervalInMillis = partitionSipMigrateIntervalInMillis;
            this.partitionBanUnregisteredSchemaMillis = partitionBanUnregisteredSchemaMillis;
            this.partitionMigrationWaitInMillis = partitionMigrationWaitInMillis;
            this.partitionSipNotifyEndOfStreamMillis = partitionSipNotifyEndOfStreamMillis;
            this.partitionRebuildEstimateActivityCountIntervalInMillis = partitionRebuildEstimateActivityCountIntervalInMillis;
        }
    }

}
