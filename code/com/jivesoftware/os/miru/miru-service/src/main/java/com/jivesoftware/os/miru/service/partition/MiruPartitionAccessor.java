package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.context.MiruRequestContext;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import com.jivesoftware.os.miru.service.stream.MiruIndexer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
public class MiruPartitionAccessor<BM> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();
    private static final int PERMITS = 64; //TODO config?

    public final MiruBitmaps<BM> bitmaps;
    public final MiruPartitionCoord coord;
    public final MiruPartitionCoordInfo info;
    public final Optional<MiruContext<BM>> context;

    public final AtomicReference<Optional<Long>> refreshTimestamp;
    public final AtomicReference<Set<TimeAndVersion>> seenLastSip;

    public final Set<Integer> beginWriters;
    public final Set<Integer> endWriters;

    public final Semaphore semaphore;
    public final AtomicBoolean closed;

    private final AtomicLong rebuildTimestamp;
    private final MiruIndexRepairs indexRepairs;
    private final MiruIndexer<BM> indexer;

    private MiruPartitionAccessor(MiruBitmaps<BM> bitmaps,
        MiruPartitionCoord coord,
        MiruPartitionCoordInfo info,
        Optional<MiruContext<BM>> context,
        AtomicLong rebuildTimestamp,
        AtomicReference<Optional<Long>> refreshTimestamp,
        Set<TimeAndVersion> seenLastSip,
        Set<Integer> beginWriters,
        Set<Integer> endWriters,
        Semaphore semaphore,
        AtomicBoolean closed,
        MiruIndexRepairs indexRepairs,
        MiruIndexer<BM> indexer) {
        this.bitmaps = bitmaps;
        this.coord = coord;
        this.info = info;
        this.context = context;
        this.rebuildTimestamp = rebuildTimestamp;
        this.refreshTimestamp = refreshTimestamp;
        this.seenLastSip = new AtomicReference<>(seenLastSip);
        this.beginWriters = beginWriters;
        this.endWriters = endWriters;
        this.semaphore = semaphore;
        this.closed = closed;
        this.indexRepairs = indexRepairs;
        this.indexer = indexer;
    }

    MiruPartitionAccessor(MiruBitmaps<BM> bitmaps,
        MiruPartitionCoord coord,
        MiruPartitionCoordInfo info,
        Optional<MiruContext<BM>> context,
        MiruIndexRepairs indexRepairs,
        MiruIndexer<BM> indexer) {
        this(bitmaps, coord, info, context, new AtomicLong(), new AtomicReference<Optional<Long>>(),
            Sets.<TimeAndVersion>newHashSet(), Sets.<Integer>newHashSet(), Sets.<Integer>newHashSet(), new Semaphore(PERMITS), new AtomicBoolean(),
            indexRepairs, indexer);
    }

    MiruPartitionAccessor<BM> copyToState(MiruPartitionState toState) {
        return new MiruPartitionAccessor<>(bitmaps, coord, info.copyToState(toState), context, rebuildTimestamp, refreshTimestamp,
            seenLastSip.get(), beginWriters, endWriters, semaphore, closed, indexRepairs, indexer);
    }

    Optional<MiruContext<BM>> close() throws InterruptedException {
        semaphore.acquire(PERMITS);
        try {
            closed.set(true);
            return context;
        } finally {
            semaphore.release(PERMITS);
        }
    }

    boolean canHotDeploy() {
        return (info.state == MiruPartitionState.offline || info.state == MiruPartitionState.bootstrap) && info.storage == MiruBackingStorage.disk;
    }

    boolean isOpenForWrites() {
        return info.state == MiruPartitionState.online;
    }

    boolean isEligibleToBackfill() {
        return info.state == MiruPartitionState.online;
    }

    boolean canAutoMigrate() {
        return info.storage == MiruBackingStorage.memory && info.state == MiruPartitionState.online;
    }

    void markForRefresh(Optional<Long> timestamp) {
        refreshTimestamp.set(timestamp);
    }

    long getRebuildTimestamp() throws IOException {
        return rebuildTimestamp.get();
    }

    void setRebuildTimestamp(long timestamp) throws IOException {
        rebuildTimestamp.set(timestamp);
    }

    long getSipTimestamp() throws IOException {
        return context.isPresent() ? context.get().sipIndex.getSip() : 0;
    }

    boolean setSipTimestamp(long timestamp) throws IOException {
        return (context.isPresent() && context.get().sipIndex.setSip(timestamp));
    }

    public static enum IndexStrategy {

        ingress, rebuild, sip;
    }

    int indexInternal(Iterator<MiruPartitionedActivity> partitionedActivities, IndexStrategy strategy, ExecutorService indexExecutor) throws Exception {
        int count = 0;
        semaphore.acquire();
        try {
            if (closed.get()) {
                return -1;
            }
            synchronized (context) {
                List<MiruPartitionedActivity> batch = new ArrayList<>();
                int activityCount = 0;
                while (partitionedActivities.hasNext()) {
                    MiruPartitionedActivity partitionedActivity = partitionedActivities.next();
                    if (partitionedActivity.partitionId.equals(coord.partitionId)) {
                        if (partitionedActivity.type == MiruPartitionedActivity.Type.BEGIN
                            || partitionedActivity.type == MiruPartitionedActivity.Type.END) {
                            activityCount += handleActivityType(batch, indexExecutor);
                            batch.clear();
                            handleBoundaryType(partitionedActivity);
                        } else if (partitionedActivity.type == MiruPartitionedActivity.Type.ACTIVITY) {
                            batch.add(partitionedActivity);
                        } else if (partitionedActivity.type == MiruPartitionedActivity.Type.REPAIR) {
                            if (strategy != IndexStrategy.rebuild) {
                                activityCount += handleActivityType(batch, indexExecutor);
                                batch.clear();
                                handleRepairType(partitionedActivity);
                            } else {
                                batch.add(partitionedActivity);
                            }
                        } else if (partitionedActivity.type == MiruPartitionedActivity.Type.REMOVE) {
                            activityCount += handleActivityType(batch, indexExecutor);
                            batch.clear();
                            handleRemoveType(partitionedActivity, strategy);
                        } else {
                            log.warn("Activity WAL contained unsupported type {}", partitionedActivity.type);
                        }

                        // This activity has been handled, so remove it from the backing list
                        partitionedActivities.remove();
                    }
                    count++;
                }
                activityCount += handleActivityType(batch, indexExecutor);
                if (activityCount > 0) {
                    indexRepairs.repaired(strategy, coord, activityCount);
                } else {
                    indexRepairs.current(strategy, coord);
                }

                if (context.isPresent()) {
                    MiruContext<BM> got = context.get();
                    log.set(ValueType.COUNT, "lastId>tenant>" + coord.tenantId + ">partition>" + coord.partitionId,
                        got.activityIndex.lastId());
                    log.set(ValueType.COUNT, "largestTimestamp>tenant>" + coord.tenantId + ">partition>" + coord.partitionId,
                        got.timeIndex.getLargestTimestamp());
                }
            }
        } finally {
            semaphore.release();
        }
        return count;
    }

    private void handleBoundaryType(MiruPartitionedActivity partitionedActivity) {
        if (partitionedActivity.type == MiruPartitionedActivity.Type.BEGIN) {
            beginWriters.add(partitionedActivity.writerId);
        } else if (partitionedActivity.type == MiruPartitionedActivity.Type.END) {
            endWriters.add(partitionedActivity.writerId);
        }
    }

    private int handleActivityType(List<MiruPartitionedActivity> partitionedActivities,
        ExecutorService indexExecutor)
        throws Exception {

        if (!context.isPresent()) {
            log.warn("Ignored activity for empty context");
            return 0;
        }

        int activityCount = 0;
        if (!partitionedActivities.isEmpty()) {
            MiruTimeIndex timeIndex = context.get().getTimeIndex();

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

            partitionedActivities.clear(); // This frees up the MiruPartitionedActivity to be garbage collected.
            if (!indexables.isEmpty()) {
                indexer.index(context.get(), indexables, indexExecutor);
                activityCount = indexables.size();
            }
        }
        return activityCount;
    }

    private void handleRepairType(MiruPartitionedActivity partitionedActivity) throws Exception {
        MiruTimeIndex timeIndex = context.get().getTimeIndex();
        MiruActivity activity = partitionedActivity.activity.get();

        int id = timeIndex.getExactId(activity.time);
        if (id < 0) {
            log.warn("Attempted to repair an activity that does not belong to this partition: {}", activity);
        } else {
            indexer.repair(context.get(), activity, id);
        }
    }

    private void handleRemoveType(MiruPartitionedActivity partitionedActivity, IndexStrategy strategy) throws Exception {
        MiruTimeIndex timeIndex = context.get().getTimeIndex();
        MiruActivity activity = partitionedActivity.activity.get();
        log.debug("Handling removal type for {} with strategy {}", activity, strategy);

        int id;
        if (strategy != IndexStrategy.rebuild || timeIndex.contains(Arrays.asList(activity.time))[0]) {
            id = timeIndex.getExactId(activity.time);
            log.trace("Removing activity for exact id {}", id);
        } else {
            id = timeIndex.nextId(activity.time)[0];
            indexer.set(context.get(), Arrays.asList(new MiruActivityAndId<>(activity, id)));
            log.trace("Removing activity for next id {}", id);
        }

        if (id < 0) {
            log.warn("Attempted to remove an activity that does not belong to this partition: {}", activity);
        } else {
            indexer.remove(context.get(), activity, id);
        }
    }

    MiruRequestHandle<BM> getRequestHandle() {
        log.debug("Request handle requested for {}", coord);
        markForRefresh(Optional.of(System.currentTimeMillis()));

        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            throw new MiruPartitionUnavailableException(e);
        }

        if (closed.get()) {
            semaphore.release();
            throw new MiruPartitionUnavailableException("Partition is closed");
        }

        return new MiruRequestHandle<BM>() {

            @Override
            public MiruBitmaps<BM> getBitmaps() {
                return bitmaps;
            }

            @Override
            public MiruRequestContext<BM> getRequestContext() {
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
            public RequestHelper getRequestHelper() {
                return null; // never talk to a local partition via reader
            }

            @Override
            public void close() throws Exception {
                semaphore.release();
            }
        };
    }

    MiruMigrationHandle<BM> getMigrationHandle(long millis) {
        try {
            semaphore.tryAcquire(millis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new MiruPartitionUnavailableException(e);
        }

        if (closed.get()) {
            semaphore.release();
            throw new MiruPartitionUnavailableException("Partition is closed");
        }

        return new MiruMigrationHandle<BM>() {

            @Override
            public boolean canMigrateTo(MiruBackingStorage destinationStorage) {
                if (info.storage == MiruBackingStorage.disk && destinationStorage == MiruBackingStorage.disk) {
                    return false;
                }
                return info.state == MiruPartitionState.online;
            }

            @Override
            public Optional<MiruContext<BM>> getContext() {
                return context;
            }

            @Override
            public MiruPartitionAccessor<BM> migrated(MiruContext<BM> context,
                Optional<MiruBackingStorage> storage,
                Optional<MiruPartitionState> state) {

                MiruPartitionCoordInfo migratedInfo = info;
                if (storage.isPresent()) {
                    migratedInfo = migratedInfo.copyToStorage(storage.get());
                }
                if (state.isPresent()) {
                    migratedInfo = migratedInfo.copyToState(state.get());
                }
                return new MiruPartitionAccessor<BM>(bitmaps, coord, migratedInfo, Optional.of(context), indexRepairs, indexer);
            }

            @Override
            public void close() throws Exception {
                semaphore.release();
            }
        };
    }

    @Override
    public String toString() {
        return "MiruPartitionAccessor{"
            + "coord=" + coord
            + ", info=" + info
            + ", rebuildTimestamp=" + rebuildTimestamp
            + ", refreshTimestamp=" + refreshTimestamp
            + ", closed=" + closed
            + '}';
    }
}
