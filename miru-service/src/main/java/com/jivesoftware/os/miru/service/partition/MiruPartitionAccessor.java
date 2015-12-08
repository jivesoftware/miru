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
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.partition.TrackError;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.service.index.Mergeable;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaActivityIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaAuthzIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaFieldIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaInboxIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaRemovalIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaSipIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaTimeIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaUnreadTrackingIndex;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.service.stream.MiruIndexer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Package protected class, for use by {@link com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition}.
 */
public class MiruPartitionAccessor<BM extends IBM, IBM, C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();
    private static final int PERMITS = 64; //TODO config?

    public final MiruStats miruStats;
    public final MiruBitmaps<BM, IBM> bitmaps;
    public final MiruPartitionCoord coord;
    public final MiruPartitionState state;
    public final boolean hasPersistentStorage;
    public final Optional<MiruContext<BM, IBM, S>> persistentContext;
    public final Optional<MiruContext<BM, IBM, S>> transientContext;

    public final AtomicReference<Set<TimeAndVersion>> seenLastSip;

    public final AtomicLong endOfStream;
    public final AtomicBoolean hasOpenWriters;

    public final Semaphore readSemaphore;
    public final Semaphore writeSemaphore;
    public final AtomicBoolean closed;

    private final AtomicReference<C> rebuildCursor;
    private final MiruIndexRepairs indexRepairs;
    private final MiruIndexer<BM, IBM> indexer;

    private final AtomicLong timestampOfLastMerge;
    private final AtomicBoolean obsolete;

    private MiruPartitionAccessor(MiruStats miruStats,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruPartitionCoord coord,
        MiruPartitionState state,
        boolean hasPersistentStorage,
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
        AtomicBoolean obsolete) {

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
    }

    static <BM extends IBM, IBM, C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> MiruPartitionAccessor<BM, IBM, C, S> initialize(MiruStats miruStats,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruPartitionCoord coord,
        MiruPartitionState state,
        boolean hasPersistentStorage,
        Optional<MiruContext<BM, IBM, S>> persistentContext,
        Optional<MiruContext<BM, IBM, S>> transientContext,
        MiruIndexRepairs indexRepairs,
        MiruIndexer<BM, IBM> indexer,
        boolean obsolete) {
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
            new AtomicBoolean(obsolete));
    }

    MiruPartitionAccessor<BM, IBM, C, S> copyToState(MiruPartitionState toState) {
        return new MiruPartitionAccessor<>(miruStats, bitmaps, coord, toState, hasPersistentStorage, persistentContext, transientContext, rebuildCursor,
            seenLastSip.get(), endOfStream, hasOpenWriters, readSemaphore, writeSemaphore, closed, indexRepairs, indexer, timestampOfLastMerge, obsolete);
    }

    void close(MiruContextFactory<S> contextFactory) throws InterruptedException {
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

    private void closeImmediate(MiruContextFactory<S> contextFactory, Optional<MiruContext<BM, IBM, S>> context) {
        if (context != null && context.isPresent()) {
            contextFactory.close(context.get());
        }
    }

    MiruBackingStorage getBackingStorage() {
        return hasPersistentStorage ? MiruBackingStorage.disk : MiruBackingStorage.memory;
    }

    boolean isCorrupt() {
        return (persistentContext.isPresent() && persistentContext.get().isCorrupt() && !transientContext.isPresent())
            || (transientContext.isPresent() && transientContext.get().isCorrupt());
    }

    public boolean isObsolete() {
        return obsolete.get();
    }

    public void markObsolete() {
        obsolete.set(true);
    }

    boolean canHotDeploy() {
        return (state == MiruPartitionState.offline || state == MiruPartitionState.bootstrap) && hasPersistentStorage;
    }

    boolean canHandleQueries() {
        return state.isOnline();
    }

    boolean isOpenForWrites() {
        return persistentContext.isPresent();
    }

    void notifyEndOfStream(long threshold) {
        if (!endOfStream.compareAndSet(0, System.currentTimeMillis() + threshold)) {
            if (endOfStream.get() < System.currentTimeMillis()) {
                hasOpenWriters.set(false);
            }
        }
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

    C getRebuildCursor() throws IOException {
        return rebuildCursor.get();
    }

    void setRebuildCursor(C cursor) throws IOException {
        rebuildCursor.set(cursor);
    }

    Optional<S> getSipCursor(StackBuffer stackBuffer) throws IOException, InterruptedException {
        return persistentContext.isPresent() ? persistentContext.get().sipIndex.getSip(stackBuffer) : null;
    }

    boolean setSip(S sip, StackBuffer stackBuffer) throws IOException, InterruptedException {
        if (sip == null) {
            throw new IllegalArgumentException("Sip cannot be null");
        }
        return (persistentContext.isPresent() && persistentContext.get().sipIndex.setSip(sip, stackBuffer));
    }

    private static class MergeRunnable implements Runnable {

        private final Mergeable mergeable;

        public MergeRunnable(Mergeable mergeable) {
            this.mergeable = mergeable;
        }

        @Override
        public void run() {
            try {
                StackBuffer stackBuffer = new StackBuffer();
                mergeable.merge(stackBuffer);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    void merge(ExecutorService mergeExecutor, Optional<MiruContext<BM, IBM, S>> context, MiruMergeChits chits, TrackError trackError) throws Exception {
        if (context.isPresent()) {
            final MiruContext<BM, IBM, S> got = context.get();
            long elapsed;
            synchronized (got.writeLock) {
                /*log.info("Merging {} (taken: {}) (remaining: {}) (merges: {})",
                coord, chits.taken(coord), chits.remaining(), merges.incrementAndGet());*/
                long start = System.currentTimeMillis();

                List<Future<?>> futures = Lists.newArrayList();
                futures.add(mergeExecutor.submit(new MergeRunnable((MiruDeltaTimeIndex) got.timeIndex)));
                for (MiruFieldType fieldType : MiruFieldType.values()) {
                    futures.add(mergeExecutor.submit(new MergeRunnable((MiruDeltaFieldIndex<BM, IBM>) got.fieldIndexProvider.getFieldIndex(fieldType))));
                }
                futures.add(mergeExecutor.submit(new MergeRunnable((MiruDeltaAuthzIndex<BM, IBM>) got.authzIndex)));
                futures.add(mergeExecutor.submit(new MergeRunnable((MiruDeltaRemovalIndex<BM, IBM>) got.removalIndex)));
                futures.add(mergeExecutor.submit(new MergeRunnable((MiruDeltaInboxIndex<BM, IBM>) got.inboxIndex)));
                futures.add(mergeExecutor.submit(new MergeRunnable((MiruDeltaUnreadTrackingIndex<BM, IBM>) got.unreadTrackingIndex)));
                futures.add(mergeExecutor.submit(new MergeRunnable((MiruDeltaActivityIndex) got.activityIndex)));
                futures.add(mergeExecutor.submit(new MergeRunnable((MiruDeltaSipIndex) got.sipIndex)));

                try {
                    for (Future<?> future : futures) {
                        future.get();
                    }
                } catch (Exception e) {
                    if (!(e instanceof InterruptedException)) {
                        trackError.error("Failed to merge: " + e.getMessage());
                    }
                    checkCorruption(got, e);
                    throw e;
                }
                timestampOfLastMerge.set(System.currentTimeMillis());
                elapsed = System.currentTimeMillis() - start;
                chits.refundAll(coord);
            }
            log.inc("merge>time>pow>" + FilerIO.chunkPower(elapsed, 0));
        }
    }

    void refundChits(MiruMergeChits mergeChits) {
        mergeChits.refundAll(coord);
    }

    public enum IndexStrategy {

        ingress, rebuild, sip;
    }

    int indexInternal(
        Optional<MiruContext<BM, IBM, S>> context,
        Iterator<MiruPartitionedActivity> partitionedActivities,
        IndexStrategy strategy,
        boolean recovery,
        MiruMergeChits chits,
        ExecutorService indexExecutor,
        ExecutorService mergeExecutor,
        TrackError trackError,
        StackBuffer stackBuffer)
        throws Exception {

        if (!context.isPresent()) {
            return -1;
        }
        MiruContext<BM, IBM, S> got = context.get();

        int consumedCount = 0;
        writeSemaphore.acquire();
        try {
            if (closed.get()) {
                return -1;
            }

            synchronized (got.writeLock) {
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
                            consumedCount += consumeTypedBatch(got, batchType, batch, strategy, indexExecutor, stackBuffer);
                            batchType = activityType;
                        }

                        batch.add(partitionedActivity);

                        // This activity has been handled, so remove it from the backing list
                        partitionedActivities.remove();
                    }

                }

                consumedCount += consumeTypedBatch(got, batchType, batch, strategy, indexExecutor, stackBuffer);
                if (consumedCount > 0) {
                    indexRepairs.repaired(strategy, coord, consumedCount);
                } else {
                    indexRepairs.current(strategy, coord);
                }

                log.set(ValueType.COUNT, "lastId>partition>" + coord.partitionId,
                    got.activityIndex.lastId(stackBuffer), coord.tenantId.toString());
                log.set(ValueType.COUNT, "largestTimestamp>partition>" + coord.partitionId,
                    got.timeIndex.getLargestTimestamp(), coord.tenantId.toString());
            }
        } catch (Exception e) {
            checkCorruption(got, e);
            throw e;
        } finally {
            writeSemaphore.release();
        }

        if (Thread.interrupted()) {
            throw new InterruptedException("Interrupted while indexing");
        }

        if (chits.take(coord, consumedCount)) {
            merge(mergeExecutor, context, chits, trackError);
        }

        return consumedCount;
    }

    private void checkCorruption(MiruContext<BM, IBM, S> got, Exception e) {
        Throwable t = e;
        while (t != null) {
            if (t instanceof CorruptionException) {
                log.warn("Corruption detected for {}: {}", coord, t.getMessage());
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
                count = handleRemoveType(got, batch, strategy, stackBuffer);
            } else {
                log.warn("Attempt to index unsupported type {}", batchType);
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
        return 0;
    }

    private int handleActivityType(MiruContext<BM, IBM, S> got,
        List<MiruPartitionedActivity> partitionedActivities,
        ExecutorService indexExecutor,
        StackBuffer stackBuffer)
        throws Exception {

        int activityCount = 0;
        if (!partitionedActivities.isEmpty()) {
            MiruTimeIndex timeIndex = got.getTimeIndex();

            List<MiruActivityAndId<MiruActivity>> indexables = new ArrayList<>(partitionedActivities.size());
            List<Long> activityTimes = new ArrayList<>();
            for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
                MiruActivity activity = partitionedActivity.activity.get();
                activityTimes.add(activity.time);
            }

            List<MiruActivity> passed = new ArrayList<>();
            boolean[] contains = timeIndex.contains(activityTimes, stackBuffer);
            for (int i = 0; i < contains.length; i++) {
                if (!contains[i]) {
                    passed.add(partitionedActivities.get(i).activity.get());
                }
            }

            if (!passed.isEmpty()) {
                long[] timestamps = new long[passed.size()];
                for (int i = 0; i < timestamps.length; i++) {
                    timestamps[i] = passed.get(i).time;
                }
                int[] ids = timeIndex.nextId(stackBuffer, timestamps);
                for (int i = 0; i < timestamps.length; i++) {
                    indexables.add(new MiruActivityAndId<>(passed.get(i), ids[i]));
                }
            }

            // free for GC before we begin indexing
            partitionedActivities.clear();
            if (!indexables.isEmpty()) {
                activityCount = indexables.size(); // indexer consumes, so count first
                indexer.index(got, coord, indexables, false, indexExecutor);
            }
        }
        return activityCount;
    }

    private int handleRepairType(MiruContext<BM, IBM, S> got,
        List<MiruPartitionedActivity> partitionedActivities,
        ExecutorService indexExecutor,
        StackBuffer stackBuffer)
        throws Exception {

        int count = 0;
        if (!partitionedActivities.isEmpty()) {
            MiruTimeIndex timeIndex = got.getTimeIndex();

            int activityCount = partitionedActivities.size();
            List<Long> activityTimes = Lists.newArrayListWithCapacity(activityCount);
            long[] timestamps = new long[activityCount];
            for (int i = 0; i < activityCount; i++) {
                MiruActivity activity = partitionedActivities.get(i).activity.get();
                activityTimes.add(activity.time);
                timestamps[i] = activity.time;
            }

            boolean[] contains = timeIndex.contains(activityTimes, stackBuffer);
            for (int i = 0; i < contains.length; i++) {
                if (contains[i]) {
                    timestamps[i] = -1;
                }
            }

            int[] ids = timeIndex.nextId(stackBuffer, timestamps);

            List<MiruActivityAndId<MiruActivity>> indexables = Lists.newArrayListWithCapacity(activityCount);
            for (int i = 0; i < activityCount; i++) {
                int id = ids[i];
                if (id == -1) {
                    id = timeIndex.getExactId(timestamps[i], stackBuffer);
                }
                if (id >= 0) {
                    indexables.add(new MiruActivityAndId<>(partitionedActivities.get(i).activity.get(), id));
                }
            }

            // free for GC before we begin indexing
            partitionedActivities.clear();
            if (!indexables.isEmpty()) {
                count = indexables.size(); // indexer consumes, so count first
                Collections.sort(indexables);
                indexer.index(got, coord, indexables, true, indexExecutor);
            }
        }
        return count;
    }

    private int handleRemoveType(MiruContext<BM, IBM, S> got,
        List<MiruPartitionedActivity> partitionedActivities,
        IndexStrategy strategy,
        StackBuffer stackBuffer)
        throws Exception {

        int count = 0;
        MiruTimeIndex timeIndex = got.getTimeIndex();
        //TODO batch remove
        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            MiruActivity activity = partitionedActivity.activity.get();
            log.debug("Handling removal type for {} with strategy {}", activity, strategy);

            int id;
            if (strategy != IndexStrategy.rebuild || timeIndex.contains(Arrays.asList(activity.time), stackBuffer)[0]) {
                id = timeIndex.getExactId(activity.time, stackBuffer);
                log.trace("Removing activity for exact id {}", id);
            } else {
                id = timeIndex.nextId(stackBuffer, activity.time)[0];
                indexer.set(got, Arrays.asList(new MiruActivityAndId<>(activity, id)));
                log.trace("Removing activity for next id {}", id);
            }

            if (id < 0) {
                log.warn("Attempted to remove an activity that does not belong to this partition: {}", activity);
            } else {
                indexer.remove(got, activity, id);
                count++;
            }
        }
        return count;
    }

    MiruRequestHandle<BM, IBM, S> getRequestHandle(TrackError trackError) {
        log.debug("Request handle requested for {}", coord);

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
            public void closePersistentContext(MiruContextFactory<S> contextFactory) {
                MiruPartitionAccessor.this.markClosed();
                MiruPartitionAccessor.this.closeImmediate(contextFactory, persistentContext);
            }

            @Override
            public void closeTransientContext(MiruContextFactory<S> contextFactory) {
                MiruPartitionAccessor.this.closeImmediate(contextFactory, transientContext);
            }

            @Override
            public void merge(ExecutorService mergeExecutor, Optional<MiruContext<BM, IBM, S>> context, MiruMergeChits chits, TrackError trackError) throws
                Exception {
                MiruPartitionAccessor.this.merge(mergeExecutor, context, chits, trackError);
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
                    newHasPersistentStorage.or(hasPersistentStorage),
                    newPersistentContext,
                    newTransientContext,
                    indexRepairs,
                    indexer,
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
