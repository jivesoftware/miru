package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.CorruptionException;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.service.stream.MiruIndexer;
import com.jivesoftware.os.miru.service.stream.MiruRebuildDirector;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Package protected class, for use by {@link com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition}.
 */
public class MiruPartitionAccessor<BM extends IBM, IBM, C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final int PERMITS = 64; //TODO config?

    public final MiruStats miruStats;
    public final MiruBitmaps<BM, IBM> bitmaps;
    public final MiruPartitionCoord coord;
    public final MiruPartitionState state;
    public final Optional<MiruContext<BM, IBM, S>> persistentContext;
    public final Optional<MiruContext<BM, IBM, S>> transientContext;

    public final AtomicReference<Set<TimeAndVersion>> seenLastSip;

    public final AtomicLong endOfStream;
    public final AtomicBoolean hasOpenWriters;
    public final AtomicReference<Boolean> hasPersistentStorage;

    public final Semaphore readSemaphore;
    public final Semaphore writeSemaphore;
    public final AtomicBoolean closed;

    private final AtomicReference<C> rebuildCursor;
    private final MiruIndexRepairs indexRepairs;
    private final MiruIndexer<BM, IBM> indexer;

    private final AtomicLong timestampOfLastMerge;
    private final AtomicBoolean obsolete;
    private final AtomicBoolean sipEndOfWAL;
    private final AtomicBoolean compactEndOfWAL;

    private MiruPartitionAccessor(MiruStats miruStats,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruPartitionCoord coord,
        MiruPartitionState state,
        AtomicReference<Boolean> hasPersistentStorage,
        Optional<MiruContext<BM, IBM, S>> persistentContext,
        Optional<MiruContext<BM, IBM, S>> transientContext,
        AtomicReference<C> rebuildCursor,
        Set<TimeAndVersion> seenLastSip,
        AtomicLong endOfStream,
        AtomicBoolean hasOpenWriters,
        Semaphore readSemaphore,
        Semaphore writeSemaphore,
        AtomicBoolean closed,
        MiruIndexRepairs indexRepairs,
        MiruIndexer<BM, IBM> indexer,
        AtomicLong timestampOfLastMerge,
        AtomicBoolean obsolete,
        AtomicBoolean sipEndOfWAL,
        AtomicBoolean compactEndOfWAL) {

        this.miruStats = miruStats;
        this.bitmaps = bitmaps;
        this.coord = coord;
        this.state = state;
        this.hasPersistentStorage = hasPersistentStorage;
        this.persistentContext = persistentContext;
        this.transientContext = transientContext;
        this.rebuildCursor = rebuildCursor;
        this.seenLastSip = new AtomicReference<>(seenLastSip);
        this.endOfStream = endOfStream;
        this.hasOpenWriters = hasOpenWriters;
        this.readSemaphore = readSemaphore;
        this.writeSemaphore = writeSemaphore;
        this.closed = closed;
        this.indexRepairs = indexRepairs;
        this.indexer = indexer;
        this.timestampOfLastMerge = timestampOfLastMerge;
        this.obsolete = obsolete;
        this.sipEndOfWAL = sipEndOfWAL;
        this.compactEndOfWAL = compactEndOfWAL;
    }

    static <BM extends IBM, IBM, C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> MiruPartitionAccessor<BM, IBM, C, S> initialize(MiruStats miruStats,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruPartitionCoord coord,
        MiruPartitionState state,
        AtomicReference<Boolean> hasPersistentStorage,
        Optional<MiruContext<BM, IBM, S>> persistentContext,
        Optional<MiruContext<BM, IBM, S>> transientContext,
        MiruIndexRepairs indexRepairs,
        MiruIndexer<BM, IBM> indexer,
        boolean obsolete,
        boolean sipEndOfWAL,
        boolean compactEndOfWAL) {
        return new MiruPartitionAccessor<BM, IBM, C, S>(miruStats,
            bitmaps,
            coord,
            state,
            hasPersistentStorage,
            persistentContext,
            transientContext,
            new AtomicReference<>(),
            Sets.<TimeAndVersion>newHashSet(),
            new AtomicLong(0),
            new AtomicBoolean(true),
            new Semaphore(PERMITS, true),
            new Semaphore(PERMITS, true),
            new AtomicBoolean(),
            indexRepairs,
            indexer,
            new AtomicLong(System.currentTimeMillis()),
            new AtomicBoolean(obsolete),
            new AtomicBoolean(sipEndOfWAL),
            new AtomicBoolean(compactEndOfWAL));
    }

    MiruPartitionAccessor<BM, IBM, C, S> copyToState(MiruPartitionState toState) {
        return new MiruPartitionAccessor<>(miruStats, bitmaps, coord, toState, hasPersistentStorage, persistentContext, transientContext, rebuildCursor,
            seenLastSip.get(), endOfStream, hasOpenWriters, readSemaphore, writeSemaphore, closed, indexRepairs, indexer, timestampOfLastMerge, obsolete,
            sipEndOfWAL, compactEndOfWAL);
    }

    void close(MiruContextFactory<S> contextFactory) throws Exception {
        writeSemaphore.acquire(PERMITS);
        try {
            readSemaphore.acquire(PERMITS);
            try {
                markClosed();
            } finally {
                readSemaphore.release(PERMITS);
            }
        } finally {
            writeSemaphore.release(PERMITS);
        }
        closeImmediate(contextFactory, persistentContext);
        closeImmediate(contextFactory, transientContext);
    }

    private void markClosed() {
        closed.set(true);
    }

    private void closeImmediate(MiruContextFactory<S> contextFactory,
        Optional<MiruContext<BM, IBM, S>> context) throws Exception {
        if (context != null && context.isPresent()) {
            contextFactory.close(context.get());
        }
    }

    void releaseRebuildTokens(MiruRebuildDirector rebuildDirector) {
        if (persistentContext.isPresent()) {
            releaseRebuildToken(persistentContext.get(), rebuildDirector);
        }
        if (transientContext.isPresent()) {
            releaseRebuildToken(transientContext.get(), rebuildDirector);
        }
    }

    private void releaseRebuildToken(MiruContext<BM, IBM, S> context, MiruRebuildDirector rebuildDirector) {
        if (context.rebuildToken != null) {
            rebuildDirector.release(context.rebuildToken);
        }
    }

    interface CheckPersistent {

        boolean check();
    }

    boolean hasPersistentStorage(CheckPersistent checkPersistent) {
        return hasPersistentStorage.updateAndGet(existing -> existing != null ? existing : checkPersistent.check());
    }

    MiruBackingStorage getBackingStorage(CheckPersistent checkPersistent) {
        return hasPersistentStorage(checkPersistent) ? MiruBackingStorage.disk : MiruBackingStorage.memory;
    }

    boolean isCorrupt() {
        return (persistentContext.isPresent() && persistentContext.get().isCorrupt() && !transientContext.isPresent())
            || (transientContext.isPresent() && transientContext.get().isCorrupt());
    }

    boolean isObsolete() {
        return obsolete.get();
    }

    void markObsolete() {
        obsolete.set(true);
    }

    void markWritersClosed() {
        hasOpenWriters.set(false);
        if (persistentContext.isPresent()) {
            persistentContext.get().markClosed();
        }
    }

    void setSipEndOfWAL(boolean sipEndOfWAL) {
        this.sipEndOfWAL.set(sipEndOfWAL);
    }

    boolean getSipEndOfWAL() {
        return sipEndOfWAL.get();
    }

    boolean updateCompactEndOfWAL(boolean from, boolean to) {
        return compactEndOfWAL.compareAndSet(from, to);
    }

    boolean getCompactEndOfWAL() {
        return compactEndOfWAL.get();
    }

    boolean canHotDeploy(CheckPersistent checkPersistent) {
        return (state == MiruPartitionState.offline || state == MiruPartitionState.bootstrap) && hasPersistentStorage(checkPersistent);
    }

    boolean canHandleQueries() {
        return state.isOnline();
    }

    boolean isOpenForWrites() {
        return persistentContext.isPresent();
    }

    boolean notifyEndOfStream(long threshold) {
        if (!endOfStream.compareAndSet(0, System.currentTimeMillis() + threshold)) {
            if (endOfStream.get() < System.currentTimeMillis()) {
                markWritersClosed();
                return true;
            }
        }
        return false;
    }

    boolean hasOpenWriters() {
        return hasOpenWriters.get();
    }

    boolean isEligibleToBackfill() {
        return state.isOnline();
    }

    boolean canAutoMigrate() {
        return transientContext.isPresent() && state == MiruPartitionState.online;
    }

    Optional<MiruRebuildDirector.Token> getRebuildToken() {
        return transientContext.isPresent() ? Optional.fromNullable(transientContext.get().rebuildToken) : Optional.<MiruRebuildDirector.Token>absent();
    }

    C getRebuildCursor() throws IOException {
        return rebuildCursor.get();
    }

    void setRebuildCursor(C cursor) throws IOException {
        rebuildCursor.set(cursor);
    }

    Optional<S> getSipCursor(StackBuffer stackBuffer) throws Exception {
        return persistentContext.isPresent() ? persistentContext.get().sipIndex.getSip(stackBuffer) : null;
    }

    boolean setSip(Optional<MiruContext<BM, IBM, S>> context, S sip, StackBuffer stackBuffer) throws Exception {
        if (sip == null) {
            throw new IllegalArgumentException("Sip cannot be null");
        }
        return (context.isPresent() && context.get().sipIndex.setSip(sip, stackBuffer));
    }

    void merge(MiruContext<BM, IBM, S> context, MiruMergeChits chits, TrackError trackError) throws Exception {
        long elapsed;
        synchronized (context.writeLock) {
            if (chits.taken(coord) == 0) {
                LOG.info("Skipped merge because no chits have been acquired for {}", coord);
                chits.refundAll(coord);
                LOG.inc("merge>skip>count");
                return;
            }
            long start = System.currentTimeMillis();

            try {
                context.commitable.commit();
            } catch (Exception e) {
                if (!(e instanceof InterruptedException)) {
                    trackError.error("Failed to merge: " + e.getMessage());
                }
                checkCorruption(context, e);
                throw e;
            }
            timestampOfLastMerge.set(System.currentTimeMillis());
            elapsed = System.currentTimeMillis() - start;
            chits.refundAll(coord);
        }
        LOG.inc("merge>time>pow>" + FilerIO.chunkPower(elapsed, 0));
    }

    void refundChits(MiruMergeChits mergeChits) {
        mergeChits.refundAll(coord);
    }

    public enum IndexStrategy {

        ingress, rebuild, sip;
    }

    int indexInternal(
        MiruContext<BM, IBM, S> context,
        Iterator<MiruPartitionedActivity> partitionedActivities,
        IndexStrategy strategy,
        boolean recovery,
        MiruMergeChits chits,
        ExecutorService indexExecutor,
        ExecutorService mergeExecutor,
        TrackError trackError,
        StackBuffer stackBuffer)
        throws Exception {

        int consumedCount = 0;
        writeSemaphore.acquire();
        try {
            if (closed.get()) {
                return -1;
            }

            synchronized (context.writeLock) {
                MiruPartitionedActivity.Type batchType = null;
                List<MiruPartitionedActivity> batch = Lists.newArrayList();
                while (partitionedActivities.hasNext()) {
                    MiruPartitionedActivity partitionedActivity = partitionedActivities.next();
                    MiruPartitionedActivity.Type activityType = partitionedActivity.type;

                    if (partitionedActivity.partitionId.equals(coord.partitionId)) {
                        // converge for simplicity
                        if (activityType == MiruPartitionedActivity.Type.ACTIVITY && recovery) {
                            activityType = MiruPartitionedActivity.Type.REPAIR;
                        } else if (activityType == MiruPartitionedActivity.Type.END) {
                            activityType = MiruPartitionedActivity.Type.BEGIN;
                        }

                        if (activityType != batchType) {
                            // this also clears the batch
                            consumedCount += consumeTypedBatch(context, batchType, batch, strategy, indexExecutor, stackBuffer);
                            batchType = activityType;
                        }

                        batch.add(partitionedActivity);

                        // This activity has been handled, so remove it from the backing list
                        partitionedActivities.remove();
                    }

                }

                consumedCount += consumeTypedBatch(context, batchType, batch, strategy, indexExecutor, stackBuffer);
                if (consumedCount > 0) {
                    indexRepairs.repaired(strategy, coord, consumedCount);
                } else {
                    indexRepairs.current(strategy, coord);
                }

                LOG.set(ValueType.COUNT, "lastId>partition>" + coord.partitionId,
                    context.activityIndex.lastId(stackBuffer), coord.tenantId.toString());
                LOG.set(ValueType.COUNT, "largestTimestamp>partition>" + coord.partitionId,
                    context.timeIndex.getLargestTimestamp(), coord.tenantId.toString());
            }
        } catch (Exception e) {
            checkCorruption(context, e);
            throw e;
        } finally {
            writeSemaphore.release();
        }

        if (Thread.interrupted()) {
            throw new InterruptedException("Interrupted while indexing");
        }

        if (chits.take(coord, consumedCount)) {
            merge(context, chits, trackError);
        }

        return consumedCount;
    }

    private void checkCorruption(MiruContext<BM, IBM, S> got, Exception e) {
        Throwable t = e;
        while (t != null) {
            if (t instanceof CorruptionException) {
                LOG.warn("Corruption detected for {}: {}", coord, t.getMessage());
                got.markCorrupt();
                break;
            }
            t = t.getCause();
        }
    }

    private int consumeTypedBatch(MiruContext<BM, IBM, S> got,
        MiruPartitionedActivity.Type batchType,
        List<MiruPartitionedActivity> batch,
        IndexStrategy strategy,
        ExecutorService indexExecutor,
        StackBuffer stackBuffer) throws Exception {

        int count = 0;
        int total = batch.size();
        if (!batch.isEmpty()) {
            long start = System.currentTimeMillis();
            if (batchType == MiruPartitionedActivity.Type.BEGIN) {
                count = handleBoundaryType(batch);
            } else if (batchType == MiruPartitionedActivity.Type.ACTIVITY) {
                count = handleActivityType(got, batch, indexExecutor, stackBuffer);
            } else if (batchType == MiruPartitionedActivity.Type.REPAIR) {
                count = handleRepairType(got, batch, indexExecutor, stackBuffer);
            } else if (batchType == MiruPartitionedActivity.Type.REMOVE) {
                count = handleRemoveType(got, batch, stackBuffer);
            } else {
                LOG.warn("Attempt to index unsupported type {}", batchType);
            }
            miruStats.ingressed(strategy.name() + ">" + coord.tenantId.toString() + ">" + coord.partitionId.getId() + ">index", count,
                System.currentTimeMillis() - start);
            miruStats.ingressed(strategy.name() + ">" + coord.tenantId.toString() + ">" + coord.partitionId.getId() + ">total", total,
                System.currentTimeMillis() - start);
            miruStats.ingressed(strategy.name() + ">" + coord.tenantId.toString() + ">" + coord.partitionId.getId() + ">calls", 1,
                System.currentTimeMillis() - start);
            batch.clear();
        }
        return count;
    }

    private int handleBoundaryType(List<MiruPartitionedActivity> partitionedActivities) {
        LOG.inc("index>boundary", partitionedActivities.size());
        return 0;
    }

    private int handleActivityType(MiruContext<BM, IBM, S> got,
        List<MiruPartitionedActivity> partitionedActivities,
        ExecutorService indexExecutor,
        StackBuffer stackBuffer)
        throws Exception {
        return handleActivities(got, partitionedActivities, indexExecutor, false, true, false, false, stackBuffer);
    }

    private int handleRepairType(MiruContext<BM, IBM, S> got,
        List<MiruPartitionedActivity> partitionedActivities,
        ExecutorService indexExecutor,
        StackBuffer stackBuffer)
        throws Exception {
        return handleActivities(got, partitionedActivities, indexExecutor, true, true, false, false, stackBuffer);
    }

    private int handleRemoveType(MiruContext<BM, IBM, S> got,
        List<MiruPartitionedActivity> partitionedActivities,
        StackBuffer stackBuffer)
        throws Exception {
        return handleActivities(got, partitionedActivities, null, false, false, true, true, stackBuffer);
    }

    private int handleActivities(MiruContext<BM, IBM, S> got,
        List<MiruPartitionedActivity> partitionedActivities,
        ExecutorService indexExecutor,
        boolean indexHits,
        boolean indexMisses,
        boolean removeHits,
        boolean removeMisses,
        StackBuffer stackBuffer)
        throws Exception {

        if (partitionedActivities.isEmpty()) {
            return 0;
        }

        // dedupe
        TLongIntMap uniques = new TLongIntHashMap(partitionedActivities.size(), 0.5f, -1L, -1);
        List<MiruPartitionedActivity> deduped = Lists.newArrayListWithCapacity(partitionedActivities.size());
        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            int existing = uniques.putIfAbsent(partitionedActivity.timestamp, deduped.size());
            if (existing == -1) {
                deduped.add(partitionedActivity);
            } else if (deduped.get(existing).activity.get().version < partitionedActivity.activity.get().version) {
                deduped.set(existing, partitionedActivity);
            }
        }
        // free for gc
        partitionedActivities.clear();
        uniques = null;
        // flip to deduped
        partitionedActivities = deduped;

        int activityCount = 0;
        MiruTimeIndex timeIndex = got.getTimeIndex();

        //List<Long> activityTimes = new ArrayList<>();
        long[] timestamps = new long[partitionedActivities.size()];
        for (int i = 0; i < partitionedActivities.size(); i++) {
            MiruActivity activity = partitionedActivities.get(i).activity.get();
            timestamps[i] = activity.time;
        }

        int[] ids = new int[timestamps.length];
        Arrays.fill(ids, -1);
        long[] monotonics = new long[timestamps.length];
        Arrays.fill(monotonics, -1);
        got.timeIdIndex.lookup(got.version, timestamps, ids, monotonics);

        int hits = 0;
        int misses = 0;
        for (int i = 0; i < ids.length; i++) {
            if (ids[i] == -1) {
                misses++;
            } else {
                hits++;
            }
        }

        MiruActivity[] hitActivities = new MiruActivity[hits];
        long[] hitTimestamps = new long[hits];
        int[] hitIds = new int[hits];
        long[] hitMonotonics = new long[hits];

        MiruActivity[] missActivities = new MiruActivity[misses];
        long[] missTimestamps = new long[misses];
        int[] missIds = new int[misses];
        long[] missMonotonics = new long[misses];

        for (int i = 0, h = 0, m = 0; i < partitionedActivities.size(); i++) {
            if (ids[i] == -1) {
                missActivities[m] = partitionedActivities.get(i).activity.get();
                missTimestamps[m] = timestamps[i];
                missIds[m] = -1;
                missMonotonics[m] = -1;
                m++;
            } else {
                hitActivities[h] = partitionedActivities.get(i).activity.get();
                hitTimestamps[h] = timestamps[i];
                hitIds[h] = ids[i];
                hitMonotonics[h] = monotonics[i];
                h++;
            }
        }

        List<MiruActivityAndId<MiruActivity>> indexables = Lists.newArrayListWithCapacity(partitionedActivities.size());

        if ((indexHits || removeHits) && hits > 0) {
            timeIndex.nextId(stackBuffer, hitTimestamps, hitIds, hitMonotonics);

            for (int i = 0; i < hits; i++) {
                if (indexHits) {
                    indexables.add(new MiruActivityAndId<>(hitActivities[i], hitIds[i], hitMonotonics[i]));
                } else {
                    indexer.remove(got, hitActivities[i], hitIds[i]);
                }
            }
        }

        if ((indexMisses || removeMisses) && misses > 0) {
            int lastIdHint = got.timeIndex.lastId();
            long largestTimestampHint = got.timeIndex.getLargestTimestamp();
            got.timeIdIndex.allocate(got.version, missTimestamps, missIds, missMonotonics, lastIdHint, largestTimestampHint);
            timeIndex.nextId(stackBuffer, missTimestamps, missIds, missMonotonics);

            for (int i = 0; i < misses; i++) {
                if (indexMisses) {
                    indexables.add(new MiruActivityAndId<>(missActivities[i], missIds[i], missMonotonics[i]));
                } else {
                    indexer.remove(got, missActivities[i], missIds[i]);
                }
            }
        }

        // free for GC before we begin indexing
        partitionedActivities.clear();
        if (!indexables.isEmpty()) {
            activityCount = indexables.size(); // indexer consumes, so count first
            Collections.sort(indexables);
            indexer.index(got, coord, indexables, indexExecutor);
        }
        return activityCount;
    }

    MiruRequestHandle<BM, IBM, S> getRequestHandle(TrackError trackError) {
        LOG.debug("Request handle requested for {}", coord);

        if (closed.get()) {
            throw new MiruPartitionUnavailableException("Partition is closed");
        }
        if (!canHandleQueries()) {
            throw new MiruPartitionUnavailableException("Partition is not online");
        }

        try {
            readSemaphore.acquire();
        } catch (InterruptedException e) {
            throw new MiruPartitionUnavailableException(e);
        }

        if (closed.get()) {
            readSemaphore.release();
            throw new MiruPartitionUnavailableException("Partition is closed");
        }

        return new MiruRequestHandle<BM, IBM, S>() {

            @Override
            public MiruBitmaps<BM, IBM> getBitmaps() {
                return bitmaps;
            }

            @Override
            public MiruRequestContext<BM, IBM, S> getRequestContext() {
                if (!state.isOnline()) {
                    throw new MiruPartitionUnavailableException("Partition is not online");
                }

                if (!persistentContext.isPresent()) {
                    throw new MiruPartitionUnavailableException("Context not set");
                }
                return persistentContext.get();
            }

            @Override
            public boolean isLocal() {
                return true;
            }

            @Override
            public boolean canBackfill() {
                return isEligibleToBackfill();
            }

            @Override
            public MiruPartitionCoord getCoord() {
                return coord;
            }

            @Override
            public TrackError getTrackError() {
                return trackError;
            }

            @Override
            public void close() throws Exception {
                readSemaphore.release();
            }

            @Override
            public void submit(ExecutorService executorService, MiruRequestHandle.AsyncQuestion<BM, IBM> asyncQuestion) {
                executorService.submit(() -> {
                    try (MiruRequestHandle<BM, IBM, S> requestHandle = getRequestHandle(trackError)) {
                        asyncQuestion.ask(requestHandle);
                    } catch (Exception x) {
                        LOG.error("Failed handling async request.", x);
                    }
                });

            }
        };
    }

    MiruMigrationHandle<BM, IBM, C, S> getMigrationHandle(long millis) {
        try {
            writeSemaphore.tryAcquire(PERMITS, millis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new MiruPartitionUnavailableException(e);
        }

        if (closed.get()) {
            writeSemaphore.release(PERMITS);
            throw new MiruPartitionUnavailableException("Partition is closed");
        }

        return new MiruMigrationHandle<BM, IBM, C, S>() {

            @Override
            public boolean canMigrateTo(MiruBackingStorage destinationStorage) {
                if (persistentContext.isPresent() && destinationStorage == MiruBackingStorage.disk) {
                    return false;
                }
                return state.isOnline();
            }

            @Override
            public Optional<MiruContext<BM, IBM, S>> getContext() {
                return persistentContext;
            }

            @Override
            public void closePersistent(MiruContextFactory<S> contextFactory) throws Exception {
                MiruPartitionAccessor.this.markClosed();
                MiruPartitionAccessor.this.closeImmediate(contextFactory, persistentContext);
            }

            @Override
            public void closeTransient(MiruContextFactory<S> contextFactory) throws Exception {
                MiruPartitionAccessor.this.closeImmediate(contextFactory, transientContext);
            }

            @Override
            public void refundChits(MiruMergeChits mergeChits) {
                MiruPartitionAccessor.this.refundChits(mergeChits);
            }

            @Override
            public void releaseRebuildTokens(MiruRebuildDirector rebuildDirector) throws Exception {
                MiruPartitionAccessor.this.releaseRebuildTokens(rebuildDirector);
            }

            @Override
            public void merge(ExecutorService mergeExecutor,
                MiruContext<BM, IBM, S> context,
                MiruMergeChits chits,
                TrackError trackError) throws Exception {
                MiruPartitionAccessor.this.merge(context, chits, trackError);
            }

            @Override
            public MiruPartitionAccessor<BM, IBM, C, S> migrated(Optional<MiruContext<BM, IBM, S>> newPersistentContext,
                Optional<MiruContext<BM, IBM, S>> newTransientContext,
                Optional<MiruPartitionState> newState,
                Optional<Boolean> newHasPersistentStorage) {

                return MiruPartitionAccessor.initialize(miruStats,
                    bitmaps,
                    coord,
                    newState.or(state),
                    newHasPersistentStorage.isPresent() ? new AtomicReference<>(newHasPersistentStorage.get()) : hasPersistentStorage,
                    newPersistentContext,
                    newTransientContext,
                    indexRepairs,
                    indexer,
                    false,
                    false,
                    false);
            }

            @Override
            public void close() throws Exception {
                writeSemaphore.release(PERMITS);
            }
        };
    }

    @Override
    public String toString() {
        return "MiruPartitionAccessor{"
            + "coord=" + coord
            + ", state=" + state
            + ", rebuildCursor=" + rebuildCursor
            + ", closed=" + closed
            + '}';
    }
}
