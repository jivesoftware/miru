package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.service.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.stream.MiruQueryStream;
import com.jivesoftware.os.miru.service.stream.MiruStream;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Package protected class, for use by {@link com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition}.
 */
class MiruPartitionStreamGate {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();
    private static final int PERMITS = 64; //TODO config?

    public final MiruPartitionCoord coord;
    public final MiruPartitionCoordInfo info;
    public final MiruStream stream;

    public final AtomicLong sipTimestamp;
    public final AtomicLong rebuildTimestamp;
    public final AtomicLong refreshTimestamp;
    public final AtomicReference<Set<TimeAndVersion>> seenLastSip;

    public final Set<Integer> beginWriters;
    public final Set<Integer> endWriters;

    public final Semaphore semaphore;
    public final AtomicBoolean closed;

    MiruPartitionStreamGate(MiruPartitionCoord coord, MiruPartitionCoordInfo info, MiruStream stream, AtomicLong sipTimestamp, AtomicLong rebuildTimestamp,
        AtomicLong refreshTimestamp, Set<TimeAndVersion> seenLastSip, Set<Integer> beginWriters, Set<Integer> endWriters, Semaphore semaphore,
        AtomicBoolean closed) {
        this.coord = coord;
        this.info = info;
        this.stream = stream;
        this.sipTimestamp = sipTimestamp;
        this.rebuildTimestamp = rebuildTimestamp;
        this.refreshTimestamp = refreshTimestamp;
        this.seenLastSip = new AtomicReference<>(seenLastSip);
        this.beginWriters = beginWriters;
        this.endWriters = endWriters;
        this.semaphore = semaphore;
        this.closed = closed;
    }

    MiruPartitionStreamGate(MiruPartitionCoord coord, MiruPartitionCoordInfo info, MiruStream stream, long sipTimestamp) {
        this(coord, info, stream, new AtomicLong(sipTimestamp), new AtomicLong(), new AtomicLong(), Sets.<TimeAndVersion>newHashSet(),
            Sets.<Integer>newHashSet(), Sets.<Integer>newHashSet(), new Semaphore(PERMITS), new AtomicBoolean());
    }

    MiruPartitionStreamGate copyToState(MiruPartitionState state) {
        return new MiruPartitionStreamGate(coord, info.copyToState(state), stream, sipTimestamp, rebuildTimestamp, refreshTimestamp, seenLastSip.get(),
            beginWriters, endWriters, semaphore, closed);
    }

    MiruStream close() throws InterruptedException {
        semaphore.acquire(PERMITS);
        try {
            closed.set(true);
            return stream;
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
        boolean needsRefresh = false;
        semaphore.acquire();
        try {
            if (closed.get()) {
                return false;
            }
            synchronized (stream) {
                while (partitionedActivities.hasNext()) {
                    MiruPartitionedActivity partitionedActivity = partitionedActivities.next();
                    if (partitionedActivity.partitionId.equals(coord.partitionId)) {
                        if (partitionedActivity.type == MiruPartitionedActivity.Type.BEGIN
                            || partitionedActivity.type == MiruPartitionedActivity.Type.END) {
                            handleBoundaryType(partitionedActivity);
                        } else if (partitionedActivity.type == MiruPartitionedActivity.Type.ACTIVITY) {
                            handleActivityType(partitionedActivity);
                            needsRefresh = true;
                        } else if (partitionedActivity.type == MiruPartitionedActivity.Type.REPAIR) {
                            if (strategy != IndexStrategy.rebuild) {
                                handleRepairType(partitionedActivity);
                            } else {
                                handleActivityType(partitionedActivity);
                            }
                            needsRefresh = true;
                        } else if (partitionedActivity.type == MiruPartitionedActivity.Type.REMOVE) {
                            handleRemoveType(partitionedActivity, strategy);
                            needsRefresh = true;
                        } else {
                            log.warn("Activity WAL contained unsupported type {}", partitionedActivity.type);
                        }

                        // This activity has been handled, so remove it from the backing list
                        partitionedActivities.remove();
                    }
                }
            }
        } finally {
            semaphore.release();
        }
        if (needsRefresh) {
            markForRefresh();
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

    private void handleActivityType(MiruPartitionedActivity partitionedActivity) throws Exception {
        MiruTimeIndex timeIndex = stream.getTimeIndex();
        MiruActivity activity = partitionedActivity.activity.get();
        if (!timeIndex.contains(activity.time)) {
            int id = timeIndex.nextId(activity.time);
            stream.getIndexStream().index(activity, id);
        }
    }

    private void handleRepairType(MiruPartitionedActivity partitionedActivity) throws Exception {
        MiruTimeIndex timeIndex = stream.getTimeIndex();
        MiruActivity activity = partitionedActivity.activity.get();

        int id = timeIndex.getExactId(activity.time);
        if (id < 0) {
            log.warn("Attempted to repair an activity that does not belong to this partition: {}", activity);
        } else {
            stream.getIndexStream().repair(activity, id);
        }
    }

    private void handleRemoveType(MiruPartitionedActivity partitionedActivity, IndexStrategy strategy) throws Exception {
        MiruTimeIndex timeIndex = stream.getTimeIndex();
        MiruActivity activity = partitionedActivity.activity.get();
        log.debug("Handling removal type for {} with strategy {}", activity, strategy);

        int id;
        if (strategy != IndexStrategy.rebuild || timeIndex.contains(activity.time)) {
            id = timeIndex.getExactId(activity.time);
            log.trace("Removing activity for exact id {}", id);
        } else {
            id = timeIndex.nextId(activity.time);
            stream.getIndexStream().set(activity, id);
            log.trace("Removing activity for next id {}", id);
        }

        if (id < 0) {
            log.warn("Attempted to remove an activity that does not belong to this partition: {}", activity);
        } else {
            stream.getIndexStream().remove(activity, id);
        }
    }

    MiruQueryHandle getQueryHandle() {
        log.debug("Query handle requested for {}", coord);
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

        return new MiruQueryHandle() {

            @Override
            public MiruQueryStream getQueryStream() {
                if (info.state != MiruPartitionState.online) {
                    throw new MiruPartitionUnavailableException("Partition is not online");
                }

                if (stream == null) {
                    throw new MiruPartitionUnavailableException("Stream not set");
                }
                return stream.getQueryStream();
            }

            @Override
            public boolean canBackfill() {
                return isEligibleToBackfill();
            }

            @Override
            public MiruPartitionId getPartitionId() {
                return coord.partitionId;
            }

            @Override
            public void close() throws Exception {
                semaphore.release();
            }
        };
    }

    MiruMigrationHandle getMigrationHandle(long millis) {
        try {
            semaphore.tryAcquire(millis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new MiruPartitionUnavailableException(e);
        }

        if (closed.get()) {
            semaphore.release();
            throw new MiruPartitionUnavailableException("Partition is closed");
        }

        return new MiruMigrationHandle() {

            @Override
            public boolean canMigrateTo(MiruBackingStorage destinationStorage) {
                return info.storage.isDiskBacked() || destinationStorage.isMemoryBacked() || isMemoryComplete();
            }

            @Override
            public MiruStream getStream() {
                return stream;
            }

            @Override
            public MiruPartitionStreamGate migrated(MiruStream stream,
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
                return new MiruPartitionStreamGate(coord, migratedInfo, stream, sipTimestamp);
            }

            @Override
            public void close() throws Exception {
                semaphore.release();
            }
        };
    }

    @Override
    public String toString() {
        return "MiruPartitionStreamGate{" +
            "coord=" + coord +
            ", info=" + info +
            ", sipTimestamp=" + sipTimestamp +
            ", rebuildTimestamp=" + rebuildTimestamp +
            ", refreshTimestamp=" + refreshTimestamp +
            ", closed=" + closed +
            '}';
    }
}
