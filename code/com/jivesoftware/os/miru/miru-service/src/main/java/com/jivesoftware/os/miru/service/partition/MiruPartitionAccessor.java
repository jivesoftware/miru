package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Package protected class, for use by {@link com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition}.
 */
class MiruPartitionAccessor<BM> {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();
    private static final int PERMITS = 64; //TODO config?

    public final MiruBitmaps<BM> bitmaps;
    public final MiruPartitionCoord coord;
    public final MiruPartitionCoordInfo info;
    public final MiruContext<BM> context;

    public final AtomicLong sipTimestamp;
    public final AtomicLong rebuildTimestamp;
    public final AtomicLong refreshTimestamp;
    public final AtomicReference<Set<TimeAndVersion>> seenLastSip;

    public final Set<Integer> beginWriters;
    public final Set<Integer> endWriters;

    public final Semaphore semaphore;
    public final AtomicBoolean closed;

    MiruPartitionAccessor(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, MiruPartitionCoordInfo info, MiruContext<BM> context, AtomicLong sipTimestamp,
        AtomicLong rebuildTimestamp, AtomicLong refreshTimestamp, Set<TimeAndVersion> seenLastSip, Set<Integer> beginWriters, Set<Integer> endWriters,
        Semaphore semaphore, AtomicBoolean closed) {
        this.bitmaps = bitmaps;
        this.coord = coord;
        this.info = info;
        this.context = context;
        this.sipTimestamp = sipTimestamp;
        this.rebuildTimestamp = rebuildTimestamp;
        this.refreshTimestamp = refreshTimestamp;
        this.seenLastSip = new AtomicReference<>(seenLastSip);
        this.beginWriters = beginWriters;
        this.endWriters = endWriters;
        this.semaphore = semaphore;
        this.closed = closed;
    }

    MiruPartitionAccessor(MiruBitmaps<BM> bitmaps, MiruPartitionCoord coord, MiruPartitionCoordInfo info, MiruContext<BM> context, long sipTimestamp) {
        this(bitmaps, coord, info, context, new AtomicLong(sipTimestamp), new AtomicLong(), new AtomicLong(), Sets.<TimeAndVersion>newHashSet(),
            Sets.<Integer>newHashSet(), Sets.<Integer>newHashSet(), new Semaphore(PERMITS), new AtomicBoolean());
    }

    MiruPartitionAccessor<BM> copyToState(MiruPartitionState state) {
        return new MiruPartitionAccessor<>(bitmaps, coord, info.copyToState(state), context, sipTimestamp, rebuildTimestamp, refreshTimestamp,
            seenLastSip.get(), beginWriters, endWriters, semaphore, closed);
    }

    MiruContext<BM> close() throws InterruptedException {
        semaphore.acquire(PERMITS);
        try {
            closed.set(true);
            return context;
        } finally {
            semaphore.release(PERMITS);
        }
    }

    boolean needsHotDeploy() {
        return info.state == MiruPartitionState.offline && info.storage.isDiskBacked();
    }

    boolean isOpenForWrites() {
        return info.storage.isMemoryBacked() && info.state == MiruPartitionState.online;
    }

    boolean isEligibleToBackfill() {
        return info.state == MiruPartitionState.online;
    }

    boolean canAutoMigrate() {
        return info.storage.isMemoryBacked() && !info.storage.isFixed() && isMemoryComplete();
    }

    boolean isMemoryComplete() {
        return info.storage.isMemoryBacked()
            && info.state == MiruPartitionState.online
            && !beginWriters.isEmpty() && beginWriters.equals(endWriters);
    }

    void markForRefresh() {
        refreshTimestamp.set(System.currentTimeMillis());
    }

    static enum IndexStrategy {

        ingress, rebuild, sip;
    }

    boolean indexInternal(Iterator<MiruPartitionedActivity> partitionedActivities, IndexStrategy strategy) throws Exception {
        semaphore.acquire();
        try {
            if (closed.get()) {
                return false;
            }
            synchronized (context) {
                List<MiruPartitionedActivity> batch = new ArrayList<>();
                while (partitionedActivities.hasNext()) {
                    MiruPartitionedActivity partitionedActivity = partitionedActivities.next();
                    if (partitionedActivity.partitionId.equals(coord.partitionId)) {
                        if (partitionedActivity.type == MiruPartitionedActivity.Type.BEGIN
                            || partitionedActivity.type == MiruPartitionedActivity.Type.END) {
                            handleActivityType(batch);
                            batch.clear();
                            handleBoundaryType(partitionedActivity);
                        } else if (partitionedActivity.type == MiruPartitionedActivity.Type.ACTIVITY) {
                            batch.add(partitionedActivity);
                        } else if (partitionedActivity.type == MiruPartitionedActivity.Type.REPAIR) {
                            if (strategy != IndexStrategy.rebuild) {
                                handleActivityType(batch);
                                batch.clear();
                                handleRepairType(partitionedActivity);
                            } else {
                                batch.add(partitionedActivity);
                            }
                        } else if (partitionedActivity.type == MiruPartitionedActivity.Type.REMOVE) {
                            handleActivityType(batch);
                            batch.clear();
                            handleRemoveType(partitionedActivity, strategy);
                        } else {
                            log.warn("Activity WAL contained unsupported type {}", partitionedActivity.type);
                        }

                        // This activity has been handled, so remove it from the backing list
                        partitionedActivities.remove();
                    }
                }
                handleActivityType(batch);

            }
        } finally {
            semaphore.release();
        }
        return true;
    }

    private void handleBoundaryType(MiruPartitionedActivity partitionedActivity) {
        if (partitionedActivity.type == MiruPartitionedActivity.Type.BEGIN) {
            beginWriters.add(partitionedActivity.writerId);
        } else if (partitionedActivity.type == MiruPartitionedActivity.Type.END) {
            endWriters.add(partitionedActivity.writerId);
        }
    }

    private void handleActivityType(List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        if (!partitionedActivities.isEmpty()) {
            MiruTimeIndex timeIndex = context.getTimeIndex();

            List<MiruActivityAndId<MiruActivity>> indexables = new ArrayList<>(partitionedActivities.size());

            for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
                MiruActivity activity = partitionedActivity.activity.get();
                if (!timeIndex.contains(activity.time)) {
                    int id = timeIndex.nextId(activity.time);
                    indexables.add(new MiruActivityAndId<>(activity, id));
                }
            }

            context.getIndexContext().index(indexables);
        }
    }

    private void handleRepairType(MiruPartitionedActivity partitionedActivity) throws Exception {
        MiruTimeIndex timeIndex = context.getTimeIndex();
        MiruActivity activity = partitionedActivity.activity.get();

        int id = timeIndex.getExactId(activity.time);
        if (id < 0) {
            log.warn("Attempted to repair an activity that does not belong to this partition: {}", activity);
        } else {
            context.getIndexContext().repair(activity, id);
        }
    }

    private void handleRemoveType(MiruPartitionedActivity partitionedActivity, IndexStrategy strategy) throws Exception {
        MiruTimeIndex timeIndex = context.getTimeIndex();
        MiruActivity activity = partitionedActivity.activity.get();
        log.debug("Handling removal type for {} with strategy {}", activity, strategy);

        int id;
        if (strategy != IndexStrategy.rebuild || timeIndex.contains(activity.time)) {
            id = timeIndex.getExactId(activity.time);
            log.trace("Removing activity for exact id {}", id);
        } else {
            id = timeIndex.nextId(activity.time);
            context.getIndexContext().set(Arrays.asList(new MiruActivityAndId<>(activity, id)));
            log.trace("Removing activity for next id {}", id);
        }

        if (id < 0) {
            log.warn("Attempted to remove an activity that does not belong to this partition: {}", activity);
        } else {
            context.getIndexContext().remove(activity, id);
        }
    }

    MiruRequestHandle<BM> getRequestHandle() {
        log.debug("Request handle requested for {}", coord);
        markForRefresh();

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

                if (context == null) {
                    throw new MiruPartitionUnavailableException("Stream not set");
                }
                return context.getQueryContext();
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
                return info.storage.isDiskBacked() || destinationStorage.isMemoryBacked() || isMemoryComplete();
            }

            @Override
            public MiruContext<BM> getStream() {
                return context;
            }

            @Override
            public MiruPartitionAccessor<BM> migrated(MiruContext<BM> stream,
                Optional<MiruBackingStorage> storage,
                Optional<MiruPartitionState> state,
                long sipTimestamp) {

                MiruPartitionCoordInfo migratedInfo = info;
                if (storage.isPresent()) {
                    migratedInfo = migratedInfo.copyToStorage(storage.get());
                }
                if (state.isPresent()) {
                    migratedInfo = migratedInfo.copyToState(state.get());
                }
                return new MiruPartitionAccessor<>(bitmaps, coord, migratedInfo, stream, sipTimestamp);
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
            + ", sipTimestamp=" + sipTimestamp
            + ", rebuildTimestamp=" + rebuildTimestamp
            + ", refreshTimestamp=" + refreshTimestamp
            + ", closed=" + closed
            + '}';
    }
}
