package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.CorruptionException;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
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
public class MiruPartitionAccessor<BM, C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();
    private static final int PERMITS = 64; //TODO config?

    public final MiruStats miruStats;
    public final MiruBitmaps<BM> bitmaps;
    public final MiruPartitionCoord coord;
    public final MiruPartitionCoordInfo info;
    public final Optional<MiruContext<BM, S>> context;

    public final AtomicReference<Set<TimeAndVersion>> seenLastSip;

    public final Set<Integer> beginWriters;
    public final Set<Integer> endWriters;

    public final Semaphore readSemaphore;
    public final Semaphore writeSemaphore;
    public final AtomicBoolean closed;

    private final AtomicReference<C> rebuildCursor;
    private final MiruIndexRepairs indexRepairs;
    private final MiruIndexer<BM> indexer;

    private final AtomicLong timestampOfLastMerge;

    private MiruPartitionAccessor(MiruStats miruStats,
        MiruBitmaps<BM> bitmaps,
        MiruPartitionCoord coord,
        MiruPartitionCoordInfo info,
        Optional<MiruContext<BM, S>> context,
        AtomicReference<C> rebuildCursor,
        Set<TimeAndVersion> seenLastSip,
        Set<Integer> beginWriters,
        Set<Integer> endWriters,
        Semaphore readSemaphore,
        Semaphore writeSemaphore,
        AtomicBoolean closed,
        MiruIndexRepairs indexRepairs,
        MiruIndexer<BM> indexer,
        AtomicLong timestampOfLastMerge) {

        this.miruStats = miruStats;
        this.bitmaps = bitmaps;
        this.coord = coord;
        this.info = info;
        this.context = context;
        this.rebuildCursor = rebuildCursor;
        this.seenLastSip = new AtomicReference<>(seenLastSip);
        this.beginWriters = beginWriters;
        this.endWriters = endWriters;
        this.readSemaphore = readSemaphore;
        this.writeSemaphore = writeSemaphore;
        this.closed = closed;
        this.indexRepairs = indexRepairs;
        this.indexer = indexer;
        this.timestampOfLastMerge = timestampOfLastMerge;
    }

    static <BM, C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> MiruPartitionAccessor<BM, C, S> initialize(MiruStats miruStats,
        MiruBitmaps<BM> bitmaps,
        MiruPartitionCoord coord,
        MiruPartitionCoordInfo info,
        Optional<MiruContext<BM, S>> context,
        MiruIndexRepairs indexRepairs,
        MiruIndexer<BM> indexer) {
        return new MiruPartitionAccessor<BM, C, S>(miruStats,
            bitmaps,
            coord,
            info,
            context,
            new AtomicReference<>(),
            Sets.<TimeAndVersion>newHashSet(),
            Sets.<Integer>newHashSet(),
            Sets.<Integer>newHashSet(),
            new Semaphore(PERMITS, true),
            new Semaphore(PERMITS, true),
            new AtomicBoolean(),
            indexRepairs,
            indexer,
            new AtomicLong(System.currentTimeMillis()));
    }

    MiruPartitionAccessor<BM, C, S> copyToState(MiruPartitionState toState) {
        return new MiruPartitionAccessor<>(miruStats, bitmaps, coord, info.copyToState(toState), context, rebuildCursor,
            seenLastSip.get(), beginWriters, endWriters, readSemaphore, writeSemaphore, closed, indexRepairs, indexer, timestampOfLastMerge);
    }

    Optional<MiruContext<BM, S>> close() throws InterruptedException {
        writeSemaphore.acquire(PERMITS);
        try {
            readSemaphore.acquire(PERMITS);
            try {
                return closeImmediate();
            } finally {
                readSemaphore.release(PERMITS);
            }
        } finally {
            writeSemaphore.release(PERMITS);
        }
    }

    private Optional<MiruContext<BM, S>> closeImmediate() {
        closed.set(true);
        return context;
    }

    boolean canHotDeploy() {
        return (info.state == MiruPartitionState.offline || info.state == MiruPartitionState.bootstrap) && info.storage == MiruBackingStorage.disk;
    }

    boolean canHandleQueries() {
        return info.state == MiruPartitionState.online;
    }

    boolean isOpenForWrites() {
        return info.state == MiruPartitionState.online;
    }

    void notifyBoundaries(List<Integer> begins, List<Integer> ends) {
        this.beginWriters.addAll(begins);
        this.endWriters.addAll(ends);
    }

    boolean hasOpenWriters() {
        return beginWriters.isEmpty() || !endWriters.containsAll(beginWriters);
    }

    boolean isEligibleToBackfill() {
        return info.state == MiruPartitionState.online;
    }

    boolean canAutoMigrate() {
        return info.storage == MiruBackingStorage.memory && info.state == MiruPartitionState.online;
    }

    C getRebuildCursor() throws IOException {
        return rebuildCursor.get();
    }

    void setRebuildCursor(C cursor) throws IOException {
        rebuildCursor.set(cursor);
    }

    Optional<S> getSipCursor() throws IOException {
        return context.isPresent() ? context.get().sipIndex.getSip() : null;
    }

    boolean setSip(S sip) throws IOException {
        if (sip == null) {
            throw new IllegalArgumentException("Sip cannot be null");
        }
        return (context.isPresent() && context.get().sipIndex.setSip(sip));
    }

    private static class MergeRunnable implements Runnable {

        private final Mergeable mergeable;

        public MergeRunnable(Mergeable mergeable) {
            this.mergeable = mergeable;
        }

        @Override
        public void run() {
            try {
                mergeable.merge();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    void merge(MiruMergeChits chits, ExecutorService mergeExecutor) throws Exception {
        if (context.isPresent()) {
            final MiruContext<BM, S> got = context.get();
            long elapsed;
            synchronized (got.writeLock) {
                long start = System.currentTimeMillis();

                List<Future<?>> futures = Lists.newArrayList();
                futures.add(mergeExecutor.submit(new MergeRunnable((MiruDeltaTimeIndex) got.timeIndex)));
                for (MiruFieldType fieldType : MiruFieldType.values()) {
                    futures.add(mergeExecutor.submit(new MergeRunnable((MiruDeltaFieldIndex<BM>) got.fieldIndexProvider.getFieldIndex(fieldType))));
                }
                futures.add(mergeExecutor.submit(new MergeRunnable((MiruDeltaAuthzIndex<BM>) got.authzIndex)));
                futures.add(mergeExecutor.submit(new MergeRunnable((MiruDeltaRemovalIndex<BM>) got.removalIndex)));
                futures.add(mergeExecutor.submit(new MergeRunnable((MiruDeltaInboxIndex<BM>) got.inboxIndex)));
                futures.add(mergeExecutor.submit(new MergeRunnable((MiruDeltaUnreadTrackingIndex<BM>) got.unreadTrackingIndex)));
                futures.add(mergeExecutor.submit(new MergeRunnable((MiruDeltaActivityIndex) got.activityIndex)));
                futures.add(mergeExecutor.submit(new MergeRunnable((MiruDeltaSipIndex) got.sipIndex)));

                try {
                    for (Future<?> future : futures) {
                        future.get();
                    }
                } catch (Exception e) {
                    checkCorruption(got, e);
                    throw e;
                }

                elapsed = System.currentTimeMillis() - start;

                chits.refundAll(coord);
                timestampOfLastMerge.set(System.currentTimeMillis());
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

    int indexInternal(Iterator<MiruPartitionedActivity> partitionedActivities,
        IndexStrategy strategy,
        boolean recovery,
        MiruMergeChits chits,
        ExecutorService indexExecutor,
        ExecutorService mergeExecutor)
        throws Exception {

        if (!context.isPresent()) {
            return -1;
        }
        MiruContext<BM, S> got = context.get();

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
                            consumedCount += consumeTypedBatch(got, batchType, batch, strategy, indexExecutor);
                            batchType = activityType;
                        }

                        batch.add(partitionedActivity);

                        // This activity has been handled, so remove it from the backing list
                        partitionedActivities.remove();
                    }

                }

                consumedCount += consumeTypedBatch(got, batchType, batch, strategy, indexExecutor);
                if (consumedCount > 0) {
                    indexRepairs.repaired(strategy, coord, consumedCount);
                } else {
                    indexRepairs.current(strategy, coord);
                }

                log.set(ValueType.COUNT, "lastId>partition>" + coord.partitionId,
                    got.activityIndex.lastId(), coord.tenantId.toString());
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
            merge(chits, mergeExecutor);
        }

        return consumedCount;
    }

    private void checkCorruption(MiruContext<BM, S> got, Exception e) {
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

    private int consumeTypedBatch(MiruContext<BM, S> got,
        MiruPartitionedActivity.Type batchType,
        List<MiruPartitionedActivity> batch,
        IndexStrategy strategy,
        ExecutorService indexExecutor) throws Exception {

        int count = 0;
        if (!batch.isEmpty()) {
            long start = System.currentTimeMillis();
            if (batchType == MiruPartitionedActivity.Type.BEGIN) {
                count = handleBoundaryType(batch);
            } else if (batchType == MiruPartitionedActivity.Type.ACTIVITY) {
                count = handleActivityType(got, batch, indexExecutor);
            } else if (batchType == MiruPartitionedActivity.Type.REPAIR) {
                count = handleRepairType(got, batch, indexExecutor);
            } else if (batchType == MiruPartitionedActivity.Type.REMOVE) {
                count = handleRemoveType(got, batch, strategy);
            } else {
                log.warn("Attempt to index unsupported type {}", batchType);
            }
            miruStats.ingressed(strategy.name() + ">" + coord.tenantId.toString() + ">" + coord.partitionId.getId(), batch.size(),
                System.currentTimeMillis() - start);
            batch.clear();

        }
        return count;
    }

    private int handleBoundaryType(List<MiruPartitionedActivity> partitionedActivities) {
        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            if (partitionedActivity.type == MiruPartitionedActivity.Type.BEGIN) {
                beginWriters.add(partitionedActivity.writerId);
            } else if (partitionedActivity.type == MiruPartitionedActivity.Type.END) {
                endWriters.add(partitionedActivity.writerId);
            }
        }
        return 0;
    }

    private int handleActivityType(MiruContext<BM, S> got,
        List<MiruPartitionedActivity> partitionedActivities,
        ExecutorService indexExecutor)
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
            boolean[] contains = timeIndex.contains(activityTimes);
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
                int[] ids = timeIndex.nextId(timestamps);
                for (int i = 0; i < timestamps.length; i++) {
                    indexables.add(new MiruActivityAndId<>(passed.get(i), ids[i]));
                }
            }

            // free for GC before we begin indexing
            partitionedActivities.clear();
            if (!indexables.isEmpty()) {
                activityCount = indexables.size(); // indexer consumes, so count first
                indexer.index(got, indexables, false, indexExecutor);
            }
        }
        return activityCount;
    }

    private int handleRepairType(MiruContext<BM, S> got,
        List<MiruPartitionedActivity> partitionedActivities,
        ExecutorService indexExecutor)
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

            boolean[] contains = timeIndex.contains(activityTimes);
            for (int i = 0; i < contains.length; i++) {
                if (contains[i]) {
                    timestamps[i] = -1;
                }
            }

            int[] ids = timeIndex.nextId(timestamps);

            List<MiruActivityAndId<MiruActivity>> indexables = Lists.newArrayListWithCapacity(activityCount);
            for (int i = 0; i < activityCount; i++) {
                int id = ids[i];
                if (id == -1) {
                    id = timeIndex.getExactId(timestamps[i]);
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
                indexer.index(got, indexables, true, indexExecutor);
            }
        }
        return count;
    }

    private int handleRemoveType(MiruContext<BM, S> got,
        List<MiruPartitionedActivity> partitionedActivities,
        IndexStrategy strategy)
        throws Exception {

        int count = 0;
        MiruTimeIndex timeIndex = got.getTimeIndex();
        //TODO batch remove
        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            MiruActivity activity = partitionedActivity.activity.get();
            log.debug("Handling removal type for {} with strategy {}", activity, strategy);

            int id;
            if (strategy != IndexStrategy.rebuild || timeIndex.contains(Arrays.asList(activity.time))[0]) {
                id = timeIndex.getExactId(activity.time);
                log.trace("Removing activity for exact id {}", id);
            } else {
                id = timeIndex.nextId(activity.time)[0];
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

    MiruRequestHandle<BM, S> getRequestHandle() {
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

        return new MiruRequestHandle<BM, S>() {

            @Override
            public MiruBitmaps<BM> getBitmaps() {
                return bitmaps;
            }

            @Override
            public MiruRequestContext<BM, S> getRequestContext() {
                if (info.state != MiruPartitionState.online) {
                    throw new MiruPartitionUnavailableException("Partition is not online");
                }

                if (!context.isPresent()) {
                    throw new MiruPartitionUnavailableException("Context not set");
                }
                return context.get();
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
            public HttpClient getClient() {
                return null; // never talk to a local partition via reader
            }

            @Override
            public void close() throws Exception {
                readSemaphore.release();
            }
        };
    }

    MiruMigrationHandle<BM, C, S> getMigrationHandle(long millis) {
        try {
            writeSemaphore.tryAcquire(PERMITS, millis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new MiruPartitionUnavailableException(e);
        }

        if (closed.get()) {
            writeSemaphore.release(PERMITS);
            throw new MiruPartitionUnavailableException("Partition is closed");
        }

        return new MiruMigrationHandle<BM, C, S>() {

            @Override
            public boolean canMigrateTo(MiruBackingStorage destinationStorage) {
                if (info.storage == MiruBackingStorage.disk && destinationStorage == MiruBackingStorage.disk) {
                    return false;
                }
                return info.state == MiruPartitionState.online;
            }

            @Override
            public Optional<MiruContext<BM, S>> getContext() {
                return context;
            }

            @Override
            public Optional<MiruContext<BM, S>> closeContext() {
                // we have all the semaphores so we can close immediately
                return MiruPartitionAccessor.this.closeImmediate();
            }

            @Override
            public void merge(MiruMergeChits chits, ExecutorService mergeExecutor) throws Exception {
                MiruPartitionAccessor.this.merge(chits, mergeExecutor);
            }

            @Override
            public MiruPartitionAccessor<BM, C, S> migrated(MiruContext<BM, S> context,
                Optional<MiruBackingStorage> storage,
                Optional<MiruPartitionState> state) {

                MiruPartitionCoordInfo migratedInfo = info;
                if (storage.isPresent()) {
                    migratedInfo = migratedInfo.copyToStorage(storage.get());
                }
                if (state.isPresent()) {
                    migratedInfo = migratedInfo.copyToState(state.get());
                }
                return MiruPartitionAccessor.initialize(miruStats, bitmaps, coord, migratedInfo, Optional.of(context), indexRepairs, indexer);
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
            + ", info=" + info
            + ", rebuildCursor=" + rebuildCursor
            + ", closed=" + closed
            + '}';
    }
}
