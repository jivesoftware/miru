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
import com.jivesoftware.os.miru.api.realtime.MiruRealtimeDelivery;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActive;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndex;
import com.jivesoftware.os.miru.plugin.index.TimeVersionRealtime;
import com.jivesoftware.os.miru.plugin.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.service.NamedThreadFactory;
import com.jivesoftware.os.miru.service.partition.MiruPartitionAccessor.CheckPersistent;
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

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruStats miruStats;
    private final MiruBitmaps<BM, IBM> bitmaps;
    private final TrackError trackError;
    private final MiruPartitionCoord coord;
    private final AtomicLong expireAfterTimestamp;
    private final MiruContextFactory<S> contextFactory;
    private final MiruSipTrackerFactory<S> sipTrackerFactory;
    private final MiruWALClient<C, S> walClient;
    private final MiruRealtimeDelivery realtimeDelivery;
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
    //private final AtomicBoolean firstRebuild = new AtomicBoolean(true);
    private final AtomicBoolean sipEndOfWAL = new AtomicBoolean(false);
    private final AtomicBoolean firstSip = new AtomicBoolean(true);
    private final CheckPersistent checkPersistent;

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
        MiruRealtimeDelivery realtimeDelivery,
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
        this.realtimeDelivery = realtimeDelivery;
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
        this.checkPersistent = () -> {
            try {
                return contextFactory.findBackingStorage(coord) == MiruBackingStorage.disk;
            } catch (Exception e) {
                throw new RuntimeException("Failed to find backing storage for " + coord, e);
            }
        };

        MiruPartitionState initialState = MiruPartitionState.offline;
        MiruBackingStorage initialStorage = contextFactory.findBackingStorage(coord);
        MiruPartitionAccessor<BM, IBM, C, S> accessor = MiruPartitionAccessor.initialize(miruStats,
            bitmaps,
            coord,
            initialState,
            new AtomicReference<>(),
            Optional.<MiruContext<BM, IBM, S>>absent(),
            Optional.<MiruContext<BM, IBM, S>>absent(),
            indexRepairs,
            indexer,
            false);

        MiruPartitionCoordInfo coordInfo = new MiruPartitionCoordInfo(initialState, initialStorage);
        heartbeatHandler.updateInfo(coord, coordInfo);
        this.accessorRef.set(accessor);
        LOG.incAtomic("state>" + initialState.name());
        LOG.incAtomic("storage>" + initialStorage.name());

        scheduledBootstrapExecutor.scheduleWithFixedDelay(
            new BootstrapRunnable(), 0, timings.partitionBootstrapIntervalInMillis, TimeUnit.MILLISECONDS);
    }

    private MiruPartitionAccessor<BM, IBM, C, S> open(MiruPartitionAccessor<BM, IBM, C, S> accessor,
        MiruPartitionState state,
        MiruRebuildDirector.Token rebuildToken) throws Exception {

        Optional<MiruContext<BM, IBM, S>> optionalPersistentContext = Optional.absent();
        Optional<MiruContext<BM, IBM, S>> optionalTransientContext = Optional.absent();

        synchronized (factoryLock) {
            MiruPartitionAccessor<BM, IBM, C, S> latestAccessor = accessorRef.get();
            if (latestAccessor != accessor) {
                LOG.warn("Ignored request for transition to state={} because the accessor changed with state={}",
                    state, latestAccessor == null ? null : latestAccessor.state);
                return latestAccessor;
            }

            boolean refundPersistentChits = false;
            boolean obsolete = false;
            if (state.isOnline() && accessor.hasPersistentStorage(checkPersistent)) {
                if (accessor.persistentContext.isPresent()) {
                    optionalPersistentContext = accessor.persistentContext;
                } else {
                    MiruSchema schema = contextFactory.loadPersistentSchema(coord);
                    MiruSchema latestSchema = contextFactory.lookupLatestSchema(coord.tenantId);
                    if (schema == null) {
                        LOG.warn("Missing schema for persistent storage on {}, marking as obsolete", coord);
                        contextFactory.markObsolete(coord);
                        obsolete = true;
                        schema = latestSchema;
                    } else if (MiruSchema.checkEquals(schema, latestSchema)) {
                        // same name and version, check if the schema itself has changed
                        if (MiruSchema.deepEquals(schema, latestSchema)) {
                            schema = latestSchema;
                        } else if (MiruSchema.checkAdditive(schema, latestSchema)) {
                            contextFactory.saveSchema(coord, latestSchema);
                            schema = latestSchema;
                        } else {
                            LOG.warn("Non-additive schema change for persistent storage on {}", coord);
                            trackError.error("Non-additive schema change for persistent storage, fix schema and restart, or force rebuild to resolve");
                        }
                    }
                    MiruContext<BM, IBM, S> context = contextFactory.allocate(bitmaps, schema, coord, MiruBackingStorage.disk, rebuildToken);
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
                    MiruContext<BM, IBM, S> context = contextFactory.allocate(bitmaps, schema, coord, MiruBackingStorage.memory, rebuildToken);
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
                if (state == MiruPartitionState.online) {
                    if (contextFactory.checkClosed(coord)) {
                        LOG.warn("Found closed context for {}", coord);
                        sipEndOfWAL.set(true);
                        opened.markWritersClosed();
                    }
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
            MiruBackingStorage updateStorage = update.getBackingStorage(checkPersistent);
            MiruPartitionCoordInfo info = new MiruPartitionCoordInfo(update.state, updateStorage);
            heartbeatHandler.updateInfo(coord, info);
            if (update.persistentContext.isPresent()) {
                int lastId = update.persistentContext.get().activityIndex.lastId(new StackBuffer());
                heartbeatHandler.updateLastId(coord, lastId);
            }

            accessorRef.set(update);

            MiruBackingStorage existingStorage = existing.getBackingStorage(checkPersistent);

            LOG.decAtomic("state>" + existing.state.name());
            LOG.decAtomic("storage>" + existingStorage.name());
            LOG.incAtomic("state>" + update.state.name());
            LOG.incAtomic("storage>" + updateStorage.name());
            if (existing.state != MiruPartitionState.bootstrap && update.state == MiruPartitionState.bootstrap) {
                bootstrapCounter.inc("Total number of partitions that need to be brought online.",
                    "Be patient. Rebalance. Increase number of concurrent rebuilds.");
            } else if (existing.state == MiruPartitionState.bootstrap && update.state != MiruPartitionState.bootstrap) {
                bootstrapCounter.dec("Total number of partitions that need to be brought online.",
                    "Be patient. Rebalance. Increase number of concurrent rebuilds.");
            }

            long transientVersion = update.transientContext.isPresent() ? update.transientContext.get().version : -1;
            long persistentVersion = update.persistentContext.isPresent() ? update.persistentContext.get().version : -1;
            LOG.info("Partition is now {}/{} for {} transient={} persistent={}", update.state, updateStorage, coord, transientVersion, persistentVersion);
            return update;
        }
    }

    @Override
    public MiruRequestHandle<BM, IBM, S> acquireQueryHandle() throws Exception {
        heartbeatHandler.updateQueryTimestamp(coord, System.currentTimeMillis());

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
        if (accessor.canHotDeploy(checkPersistent)) {
            LOG.info("Hot deploying for query: {}", coord);
            accessor = open(accessor, MiruPartitionState.online, null);
        }
        return accessor.getRequestHandle(trackError);
    }

    @Override
    public MiruRequestHandle<BM, IBM, S> inspectRequestHandle(boolean hotDeploy) throws Exception {
        if (removed.get()) {
            throw new MiruPartitionUnavailableException("Partition has been removed");
        }

        MiruPartitionAccessor<BM, IBM, C, S> accessor = accessorRef.get();
        if (hotDeploy) {
            heartbeatHandler.updateQueryTimestamp(coord, System.currentTimeMillis());

            if (accessor.canHotDeploy(checkPersistent)) {
                LOG.info("Hot deploying for query: {}", coord);
                accessor = open(accessor, MiruPartitionState.online, null);
            }
        }

        return accessor.getRequestHandle(trackError);
    }

    @Override
    public boolean isAvailable() {
        return !removed.get() && sipEndOfWAL.get();
    }

    @Override
    public void remove() throws Exception {
        LOG.info("Removing partition by request: {}", coord);
        removed.set(true);
        close();
    }

    private boolean close() throws Exception {
        try {

            MiruPartitionAccessor<BM, IBM, C, S> existing = accessorRef.get();
            Optional<MiruContext<BM, IBM, S>> persistentContext = existing.persistentContext;
            if (persistentContext.isPresent()) {
                existing.merge(persistentMergeExecutor, persistentContext.get(), persistentMergeChits, trackError);
            }
            synchronized (factoryLock) {
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
                    existing.close(contextFactory);
                    existing.releaseRebuildTokens(rebuildDirector);
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
        return accessorRef.get().getBackingStorage(checkPersistent);
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

            int count = 0;
            if (context.isPresent()) {
                count = accessor.indexInternal(context.get(),
                    partitionedActivities,
                    MiruPartitionAccessor.IndexStrategy.ingress,
                    false,
                    mergeChits,
                    sameThreadExecutor,
                    sameThreadExecutor,
                    trackError,
                    stackBuffer);
            }
            if (count > 0) {
                LOG.inc("indexIngress>written", count);
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
            LOG.inc("indexIngress>dropped", count);
        }
    }

    @Override
    public void warm() throws Exception {
        heartbeatHandler.updateQueryTimestamp(coord, System.currentTimeMillis());
        LOG.inc("warm", 1);
        LOG.inc("warm", 1, coord.tenantId.toString());
        LOG.inc("warm>partition>" + coord.partitionId, 1, coord.tenantId.toString());
    }

    @Override
    public boolean rebuild() throws Exception {
        MiruPartitionAccessor<BM, IBM, C, S> accessor = accessorRef.get();
        return !accessor.transientContext.isPresent() && rebuild(accessor);
    }

    private boolean rebuild(MiruPartitionAccessor<BM, IBM, C, S> accessor) throws Exception {
        synchronized (factoryLock) {
            boolean updated = false;
            try (MiruMigrationHandle<BM, IBM, C, S> handle = accessor.getMigrationHandle(timings.partitionMigrationWaitInMillis)) {
                // make sure the accessor didn't change while getting the handle, and that it's ready to migrate
                if (accessorRef.get() == accessor) {
                    if (accessor.transientContext.isPresent()) {
                        handle.closeTransient(contextFactory);
                        handle.releaseRebuildTokens(rebuildDirector);
                        handle.refundChits(transientMergeChits);
                        contextFactory.remove(accessor.transientContext.get());
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

                    handle.closePersistent(contextFactory);
                    contextFactory.cleanDisk(coord);

                    MiruContext<BM, IBM, S> toContext;
                    MiruContext<BM, IBM, S> fromContext = accessor.transientContext.get();
                    synchronized (fromContext.writeLock) {
                        handle.merge(transientMergeExecutor, fromContext, transientMergeChits, trackError);
                        handle.closeTransient(contextFactory);
                        toContext = contextFactory.copy(bitmaps, fromContext.schema, coord, fromContext, MiruBackingStorage.disk, stackBuffer);
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
                            LOG.info("Partition at {} has transitioned to persistent storage", coord);
                            contextFactory.remove(fromContext);
                            handle.releaseRebuildTokens(rebuildDirector);

                            updated = migrated;
                        } else {
                            LOG.warn("Partition at {} failed to migrate to {}, attempting to rewind", coord, MiruBackingStorage.disk);
                            contextFactory.remove(toContext);
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
        private final AtomicBoolean destroyed = new AtomicBoolean();

        @Override
        public void run() {
            try {
                try {
                    checkActive();
                } catch (MiruSchemaUnvailableException sue) {
                    LOG.warn("Tenant is active but schema not available for {}", coord.tenantId);
                    LOG.debug("Tenant is active but schema not available", sue);
                } catch (Throwable t) {
                    LOG.error("CheckActive encountered a problem for {}", new Object[] { coord }, t);
                }
            } catch (Throwable t) {
                LOG.error("Bootstrap encountered a problem for {}", new Object[] { coord }, t);
            }
        }

        private void checkActive() throws Exception {
            if (removed.get() || banUnregisteredSchema.get() >= System.currentTimeMillis()) {
                return;
            }

            MiruPartitionAccessor<BM, IBM, C, S> accessor = accessorRef.get();
            MiruPartitionActive partitionActive = heartbeatHandler.getPartitionActive(coord);
            if (partitionActive.destroyAfterTimestamp > 0 && System.currentTimeMillis() > partitionActive.destroyAfterTimestamp) {
                if (accessor.state != MiruPartitionState.offline) {
                    LOG.info("Taking partition offline because it is marked for destruction: {}", coord);
                    close();
                    accessor = accessorRef.get();
                }
                //TODO this guarantees we will always check persistent storage on restart for every destroyed partition ever - need to make this super lazy
                boolean acquired = !destroyed.get() && heartbeatHandler.acquireDestructionHandle();
                if (acquired) {
                    try {
                        if (accessor.state == MiruPartitionState.offline && accessor.hasPersistentStorage(checkPersistent)) {
                            synchronized (factoryLock) {
                                LOG.info("Cleaning disk for partition because it is marked for destruction: {}", coord);

                                MiruPartitionAccessor<BM, IBM, C, S> existing = accessorRef.get();

                                MiruPartitionAccessor<BM, IBM, C, S> cleaned = MiruPartitionAccessor.initialize(miruStats,
                                    bitmaps,
                                    coord,
                                    MiruPartitionState.offline,
                                    new AtomicReference<>(false),
                                    Optional.<MiruContext<BM, IBM, S>>absent(),
                                    Optional.<MiruContext<BM, IBM, S>>absent(),
                                    indexRepairs,
                                    indexer,
                                    false);
                                cleaned = updatePartition(existing, cleaned);
                                if (cleaned != null) {
                                    existing.close(contextFactory);
                                    existing.releaseRebuildTokens(rebuildDirector);
                                    existing.refundChits(persistentMergeChits);
                                    existing.refundChits(transientMergeChits);
                                    clearFutures();
                                    contextFactory.cleanDisk(coord);
                                    destroyed.set(true);
                                } else {
                                    LOG.warn("Failed to clean disk because accessor changed for partition: {}", coord);
                                }
                            }
                        }
                    } finally {
                        heartbeatHandler.releaseDestructionHandle();
                    }
                }
            } else if (partitionActive.activeUntilTimestamp > System.currentTimeMillis()) {
                if (accessor.state == MiruPartitionState.offline) {
                    if (accessor.hasPersistentStorage(checkPersistent)) {
                        if (!removed.get() && accessor.canHotDeploy(checkPersistent)) {
                            LOG.info("Hot deploying for checkActive: {}", coord);
                            open(accessor, MiruPartitionState.online, null);
                        }
                    } else {
                        try {
                            open(accessor, MiruPartitionState.bootstrap, null);
                        } catch (MiruPartitionUnavailableException e) {
                            LOG.warn("CheckActive: Partition is active for tenant {} but no schema is registered, banning for {} ms",
                                coord.tenantId, timings.partitionBanUnregisteredSchemaMillis);
                            banUnregisteredSchema.set(System.currentTimeMillis() + timings.partitionBanUnregisteredSchemaMillis);
                        }
                    }
                } else if (accessor.persistentContext.isPresent() && System.currentTimeMillis() > partitionActive.idleAfterTimestamp) {
                    contextFactory.releaseCaches(accessor.persistentContext.get());
                }
            } else if (accessor.state != MiruPartitionState.offline) {
                if (accessor.transientContext.isPresent()) {
                    LOG.info("Partition {} is idle but still has a transient context, closure will be deferred", coord);
                } else if (accessor.persistentContext.isPresent()) {
                    int deliveryId = -1;
                    int lastId = -1;
                    try (MiruRequestHandle<BM, IBM, S> handle = accessor.getRequestHandle(trackError)) {
                        StackBuffer stackBuffer = new StackBuffer();
                        deliveryId = handle.getRequestContext().getSipIndex().getRealtimeDeliveryId(stackBuffer);
                        lastId = handle.getRequestContext().getActivityIndex().lastId(stackBuffer);
                    } catch (Throwable t) {
                        LOG.error("Failed to check realtime delivery id for inactive partition {}", new Object[] { coord }, t);
                    }
                    if (deliveryId == lastId) {
                        close();
                    } else {
                        LOG.warn("Partition {} is inactive but realtime deliveryId {} needs to catch up with lastId {}", coord, deliveryId, lastId);
                    }
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
                                accessor = open(accessor, desiredState, token.get());
                                if (accessor.state != desiredState) {
                                    LOG.warn("Failed to transition to {} for {}", desiredState, coord);
                                    accessor = null;
                                }
                            }
                            if (accessor != null) {
                                try {
                                    if (accessor.transientContext.isPresent()) {
                                        MiruContext<BM, IBM, S> got = accessor.transientContext.get();
                                        if (got.isCorrupt()) {
                                            if (close()) {
                                                LOG.warn("Stopped rebuild due to corruption for {}", coord);
                                            } else {
                                                LOG.error("Failed to stop rebuild after corruption for {}", coord);
                                            }
                                        } else if (rebuild(accessor, stackBuffer)) {
                                            MiruPartitionAccessor<BM, IBM, C, S> online = accessor.copyToState(MiruPartitionState.online);
                                            accessor = updatePartition(accessor, online);
                                            if (accessor != null) {
                                                accessor.merge(transientMergeExecutor, got, transientMergeChits, trackError);
                                                trackError.reset();
                                            }
                                        } else {
                                            LOG.error("Rebuild did not finish for {} isAccessor={}", coord, (accessor == accessorRef.get()));
                                        }
                                    } else {
                                        LOG.error("Attempted rebuild without a transientContext for {}", coord);
                                    }
                                } catch (Throwable t) {
                                    LOG.error("Rebuild encountered a problem for {}", new Object[] { coord }, t);
                                }
                            }
                        } catch (MiruPartitionUnavailableException e) {
                            LOG.warn("Rebuild: Partition is active for tenant {} but no schema is registered, banning for {} ms",
                                coord.tenantId, timings.partitionBanUnregisteredSchemaMillis);
                            banUnregisteredSchema.set(System.currentTimeMillis() + timings.partitionBanUnregisteredSchemaMillis);
                        } finally {
                            if (accessor == null || !accessor.getRebuildToken().isPresent()) {
                                rebuildDirector.release(token.get());
                            }
                        }
                    } else {
                        LOG.debug("Skipped rebuild because count={} available={}", count, rebuildDirector.available());
                    }
                }
            } catch (MiruSchemaUnvailableException e) {
                LOG.warn("Skipped rebuild because schema is unavailable for {}", coord);
            } catch (Throwable t) {
                LOG.error("RebuildIndex encountered a problem for {}", new Object[] { coord }, t);
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

            LOG.debug("Starting rebuild at {} for {}", cursor.get(), coord);

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
                    LOG.debug("Signaling end of rebuild for {}", coord);
                    endOfWAL.set(true);
                } catch (Exception x) {
                    LOG.error("Failure while rebuilding {}", new Object[] { coord }, x);
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

                    if (streamBatch != null && streamBatch.cursor != null) {
                        partitionedActivities = new ArrayList<>(streamBatch.activities.size());
                        for (MiruWALEntry batch : streamBatch.activities) {
                            partitionedActivities.add(batch.activity);
                        }
                        nextCursor = streamBatch.cursor;
                    } else {
                        // end of rebuild
                        LOG.debug("Ending rebuild for {}", coord);
                        break;
                    }

                    int count = partitionedActivities.size();
                    totalIndexed += count;

                    LOG.debug("Indexing batch of size {} (total {}) for {}", count, totalIndexed, coord);
                    LOG.startTimer("rebuild>batchSize-" + partitionRebuildBatchSize);
                    if (accessor.transientContext.isPresent()) {
                        accessor.indexInternal(accessor.transientContext.get(),
                            partitionedActivities.iterator(),
                            MiruPartitionAccessor.IndexStrategy.rebuild,
                            false,
                            transientMergeChits,
                            rebuildIndexExecutor,
                            transientMergeExecutor,
                            trackError,
                            stackBuffer);
                    }
                    //accessor.merge(transientMergeExecutor, accessor.transientContext, transientMergeChits, trackError);
                    accessor.setRebuildCursor(nextCursor);
                    if (nextCursor.getSipCursor() != null) {
                        accessor.setSip(accessor.transientContext, nextCursor.getSipCursor(), stackBuffer);
                    }

                    LOG.stopTimer("rebuild>batchSize-" + partitionRebuildBatchSize);
                    LOG.inc("rebuild>count>calls", 1);
                    LOG.inc("rebuild>count>total", count);
                    LOG.inc("rebuild>count>power>" + FilerIO.chunkPower(count, 0), 1);
                    LOG.inc("rebuild", count, coord.tenantId.toString());
                    LOG.inc("rebuild>partition>" + coord.partitionId, count, coord.tenantId.toString());
                }
            } catch (Exception e) {
                LOG.error("Failure during rebuild index for {}", new Object[] { coord }, e);
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
                    LOG.warn("Ignored interrupt while awaiting shutdown of rebuild index executor for {}", coord);
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
        private final AtomicLong updatedLastId = new AtomicLong(-1);

        @Override
        public void run() {
            StackBuffer stackBuffer = new StackBuffer();
            try {
                MiruPartitionAccessor<BM, IBM, C, S> accessor = accessorRef.get();
                MiruPartitionState state = accessor.state;
                if (state.isOnline()) {
                    checkObsolete(accessor);
                    updateLastId(accessor, stackBuffer);

                    boolean forceRebuild = false;
                    if (!accessor.persistentContext.isPresent() && !accessor.transientContext.isPresent()) {
                        LOG.info("Forcing rebuild because context is missing for {}", coord);
                        forceRebuild = true;
                    } else if (accessor.isCorrupt()) {
                        LOG.info("Forcing rebuild because context is corrupt for {}", coord);
                        forceRebuild = true;
                    } else if (!accessor.transientContext.isPresent() && accessor.isObsolete() && accessor.state != MiruPartitionState.obsolete) {
                        LOG.info("Forcing rebuild because context is obsolete for {}", coord);
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
                        LOG.error("Migrate encountered a problem for {}", new Object[] { coord }, t);
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

                            SipResult sipResult = sip(accessor, stackBuffer);
                            if (sipResult.sippedEndOfWAL) {
                                sipEndOfWAL.set(true);
                            }
                            if (sipResult.sippedEndOfStream) {
                                synchronized (factoryLock) {
                                    if (accessor == accessorRef.get() && !accessor.hasOpenWriters() && persistentMergeChits.taken(coord) == 0) {
                                        contextFactory.markClosed(coord);
                                    }
                                }
                            }
                        } else {
                            accessor.refundChits(persistentMergeChits);
                        }
                    } catch (Throwable t) {
                        LOG.error("Sip encountered a problem for {}", new Object[] { coord }, t);
                    }
                    try {
                        deliverRealtime("sip", accessor, stackBuffer);
                    } catch (Throwable t) {
                        LOG.error("Sip realtime delivery encountered a problem for {}", new Object[] { coord }, t);
                    }
                }
            } catch (Throwable t) {
                LOG.error("SipMigrateIndex encountered a problem for {}", new Object[] { coord }, t);
            }
        }

        private void checkObsolete(MiruPartitionAccessor<BM, IBM, C, S> accessor) throws Exception {
            if (accessor.transientContext.isPresent()) {
                return;
            }

            if (checkedObsolete.compareAndSet(false, true)) {
                if (contextFactory.checkObsolete(coord)) {
                    LOG.warn("Found obsolete context for {}", coord);
                    accessor.markObsolete();
                    return;
                }
            }

            if (accessor.persistentContext.isPresent() && !accessor.isObsolete()) {
                MiruSchema latestSchema = contextFactory.lookupLatestSchema(coord.tenantId);
                MiruContext<BM, IBM, S> context = accessor.persistentContext.get();
                if (!MiruSchema.checkEquals(latestSchema, context.schema)) {
                    LOG.warn("Found obsolete schema for {}: {} {} vs {} {}",
                        coord, latestSchema.getName(), latestSchema.getVersion(), context.schema.getName(), context.schema.getVersion());
                    accessor.markObsolete();
                }
            }
        }

        private void updateLastId(MiruPartitionAccessor<BM, IBM, C, S> accessor, StackBuffer stackBuffer) throws Exception {
            if (accessor.transientContext.isPresent()) {
                return;
            }

            if (accessor.persistentContext.isPresent() && updatedLastId.get() == -1) {
                int lastId = accessor.persistentContext.get().activityIndex.lastId(stackBuffer);
                heartbeatHandler.updateLastId(coord, lastId);
                updatedLastId.set(lastId);
            }
        }

        private SipResult sip(final MiruPartitionAccessor<BM, IBM, C, S> accessor, StackBuffer stackBuffer) throws Exception {

            final MiruSipTracker<S> sipTracker = sipTrackerFactory.create(accessor.seenLastSip.get());

            S sipCursor = accessor.getSipCursor(stackBuffer).orNull();
            boolean first = firstSip.get();

            MiruWALClient.StreamBatch<MiruWALEntry, S> sippedActivity = walClient.sipActivity(coord.tenantId,
                coord.partitionId,
                sipCursor,
                sipTracker.getSeenLastSip(),
                partitionSipBatchSize);

            boolean sippedEndOfWAL = false;
            boolean sippedEndOfStream = false;

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
                    LOG.info("Sipped to end of stream for {}", coord);
                    long threshold = first ? 0 : timings.partitionSipNotifyEndOfStreamMillis;
                    sippedEndOfWAL = true;
                    sippedEndOfStream = accessor.notifyEndOfStream(threshold);
                    break;
                } else if (sipCursor == null) {
                    LOG.warn("No cursor for {}", coord);
                    sippedEndOfWAL = true;
                    break;
                } else if (sipCursor.equals(lastCursor)) {
                    LOG.debug("Sipped same cursor for {}", coord);
                    sippedEndOfWAL = true;
                    break;
                } else if (sippedActivity.endOfWAL) {
                    LOG.debug("Sipped to end of WAL for {}", coord);
                    sippedEndOfWAL = true;
                    break;
                }

                sippedActivity = walClient.sipActivity(coord.tenantId, coord.partitionId, sipCursor, sipTracker.getSeenThisSip(), partitionSipBatchSize);
            }

            if (!accessor.hasOpenWriters() && accessor.persistentContext.isPresent()) {
                LOG.info("Forcing merge after sip with no open writers for coord:{} sippedEndOfWAL:{} sippedEndOfStream:{}",
                    coord, sippedEndOfWAL, sippedEndOfStream);
                accessor.merge(persistentMergeExecutor, accessor.persistentContext.get(), persistentMergeChits, trackError);
            }

            return new SipResult(sippedEndOfWAL, sippedEndOfStream);
        }

        private class SipResult {

            private final boolean sippedEndOfWAL;
            private final boolean sippedEndOfStream;

            public SipResult(boolean sippedEndOfWAL, boolean sippedEndOfStream) {
                this.sippedEndOfWAL = sippedEndOfWAL;
                this.sippedEndOfStream = sippedEndOfStream;
            }
        }

        private S deliver(final List<MiruPartitionedActivity> partitionedActivities, final MiruPartitionAccessor<BM, IBM, C, S> accessor,
            final MiruSipTracker<S> sipTracker, S sipCursor, S nextSipCursor, StackBuffer stackBuffer) throws Exception {
            boolean repair = firstSip.compareAndSet(true, false);
            int initialCount = partitionedActivities.size();
            int count = 0;
            if (accessor.persistentContext.isPresent()) {
                count = accessor.indexInternal(
                    accessor.persistentContext.get(),
                    partitionedActivities.iterator(),
                    MiruPartitionAccessor.IndexStrategy.sip,
                    repair,
                    persistentMergeChits,
                    sipIndexExecutor,
                    persistentMergeExecutor,
                    trackError,
                    stackBuffer);
                int lastId = accessor.persistentContext.get().activityIndex.lastId(stackBuffer);
                if (lastId != updatedLastId.get()) {
                    heartbeatHandler.updateLastId(coord, lastId);
                    updatedLastId.set(lastId);
                }
            }

            S suggestion = sipTracker.suggest(sipCursor, nextSipCursor);
            if (suggestion != null && accessor.setSip(accessor.persistentContext, suggestion, stackBuffer)) {
                accessor.seenLastSip.compareAndSet(sipTracker.getSeenLastSip(), sipTracker.getSeenThisSip());
                sipTracker.metrics(coord, suggestion);
            }

            LOG.inc("sip>count>calls", 1);
            if (count > 0) {
                LOG.inc("sip>count>total", count);
                LOG.inc("sip>count>power>" + FilerIO.chunkPower(count, 0), 1);
                LOG.inc("sip>count", count, coord.tenantId.toString());
                LOG.inc("sip>partition>" + coord.partitionId, count, coord.tenantId.toString());
            }
            if (initialCount > 0) {
                if (initialCount > count * 2 && accessor.persistentContext.isPresent()) {
                    MiruContext<BM, IBM, S> context = accessor.persistentContext.get();
                    LOG.debug("Partition skipped over half a batch for {} while sipping, hasChunkStores={} hasLabIndex={} lastId={} cursor={} nextCursor={}",
                        coord, context.hasChunkStores(), context.hasLabIndex(), context.timeIndex.lastId(), sipCursor, nextSipCursor);
                }
                LOG.inc("sip>count>skip", (initialCount - count));
            }
            return suggestion;
        }

    }

    private void deliverRealtime(String name, MiruPartitionAccessor<BM, IBM, C, S> accessor, StackBuffer stackBuffer) throws Exception {
        if (!accessor.persistentContext.isPresent()) {
            return;
        }
        int count = 0;
        try (MiruRequestHandle<BM, IBM, S> handle = accessor.getRequestHandle(trackError)) {
            MiruSipIndex<S> sipIndex = handle.getRequestContext().getSipIndex();
            MiruActivityIndex activityIndex = handle.getRequestContext().getActivityIndex();
            int deliveryId = sipIndex.getRealtimeDeliveryId(stackBuffer);
            int lastId = activityIndex.lastId(stackBuffer);
            if (lastId > deliveryId) {
                List<Long> activityTimes = Lists.newArrayList();
                int gathered = 0;
                for (int id = deliveryId + 1; id <= lastId; id += partitionSipBatchSize) {
                    int batchSize = Math.min(partitionSipBatchSize, lastId - id + 1);
                    int[] indexes = new int[batchSize];
                    for (int i = 0; i < batchSize; i++) {
                        indexes[i] = id + i;
                    }
                    TimeVersionRealtime[] timeVersionRealtimes = activityIndex.getAllTimeVersionRealtime("sipRealtime", indexes, stackBuffer);
                    for (int i = 0; i < timeVersionRealtimes.length; i++) {
                        TimeVersionRealtime tvr = timeVersionRealtimes[i];
                        if (tvr == null) {
                            LOG.warn("Missing realtime info at index:{} batch:{} offset:{} deliveryId:{} lastId:{} gathered:{} sent:{}",
                                indexes[i], batchSize, i, deliveryId, lastId, gathered, activityTimes.size());
                            continue;
                        }
                        gathered++;
                        if (tvr.realtimeDelivery) {
                            activityTimes.add(tvr.timestamp);
                            count++;
                        }
                    }
                    if (activityTimes.size() >= partitionSipBatchSize) {
                        realtimeDelivery.deliver(coord, activityTimes);
                        activityTimes.clear();
                        sipIndex.setRealtimeDeliveryId(lastId, stackBuffer);
                    }
                }

                if (!activityTimes.isEmpty()) {
                    realtimeDelivery.deliver(coord, activityTimes);
                    sipIndex.setRealtimeDeliveryId(lastId, stackBuffer);
                }
                LOG.info("Delivered realtime for coord:{} deliveryId:{} lastId:{} gathered:{} sent:{}",
                    coord, deliveryId, lastId, gathered, activityTimes.size());
            }
        }
        LOG.inc("deliver>realtime>" + name + ">calls", 1);
        LOG.inc("deliver>realtime>" + name + ">total", count);
        LOG.inc("deliver>realtime>" + name + ">power>" + FilerIO.chunkPower(count, 0), 1);
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
