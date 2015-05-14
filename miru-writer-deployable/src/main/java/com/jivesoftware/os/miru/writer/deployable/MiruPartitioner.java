package com.jivesoftware.os.miru.writer.deployable;

import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.lookup.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.miru.wal.lookup.MiruWALLookup;
import com.jivesoftware.os.miru.wal.partition.MiruPartitionCursor;
import com.jivesoftware.os.miru.wal.partition.MiruPartitionIdProvider;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALWriter;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** @author jonathan */
public class MiruPartitioner {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final int writerId;
    private final MiruPartitionIdProvider partitionIdProvider;
    private final MiruActivityWALWriter activityWALWriter;
    private final MiruActivityWALReader<?, ?> activityWALReader;
    private final MiruReadTrackingWALWriter readTrackingWAL;
    private final MiruWALLookup walLookup;
    private final long partitionMaximumAgeInMillis;
    private final MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();
    private final StripingLocksProvider<MiruTenantId> locks = new StripingLocksProvider<>(64);

    public MiruPartitioner(int writerId,
        MiruPartitionIdProvider partitionIdProvider,
        MiruActivityWALWriter activityWALWriter,
        MiruActivityWALReader<?, ?> activityWALReader,
        MiruReadTrackingWALWriter readTrackingWAL,
        MiruWALLookup walLookup,
        long partitionMaximumAgeInMillis) {
        this.writerId = writerId;
        this.partitionIdProvider = partitionIdProvider;
        this.activityWALWriter = activityWALWriter;
        this.activityWALReader = activityWALReader;
        this.readTrackingWAL = readTrackingWAL;
        this.walLookup = walLookup;
        this.partitionMaximumAgeInMillis = partitionMaximumAgeInMillis;
    }

    public void checkForAlignmentWithOtherWriters(MiruTenantId tenantId) throws Exception {
        synchronized (locks.lock(tenantId)) {
            MiruPartitionId largestPartitionIdAcrossAllWriters = partitionIdProvider.getLargestPartitionIdAcrossAllWriters(tenantId);
            MiruPartitionCursor partitionCursor = partitionIdProvider.getCursor(tenantId, writerId);
            MiruPartitionId currentPartitionId = partitionCursor.getPartitionId();
            if (currentPartitionId.compareTo(largestPartitionIdAcrossAllWriters) < 0) {
                List<MiruPartitionedActivity> partitionedActivities = new ArrayList<>();
                for (MiruPartitionId partitionId = currentPartitionId;
                     partitionId.compareTo(largestPartitionIdAcrossAllWriters) < 0;
                     partitionId = partitionId.next()) {

                    int latestIndex = partitionIdProvider.getLatestIndex(tenantId, partitionId, writerId);
                    partitionedActivities.add(partitionedActivityFactory.begin(writerId, partitionId, tenantId, latestIndex));
                    partitionedActivities.add(partitionedActivityFactory.end(writerId, partitionId, tenantId, latestIndex));
                }

                int latestIndex = partitionIdProvider.getLatestIndex(tenantId, largestPartitionIdAcrossAllWriters, writerId);
                partitionedActivities.add(partitionedActivityFactory.begin(writerId, largestPartitionIdAcrossAllWriters, tenantId, latestIndex));

                flush(tenantId, largestPartitionIdAcrossAllWriters, true, partitionedActivities);
                partitionIdProvider.setLargestPartitionIdForWriter(tenantId, largestPartitionIdAcrossAllWriters, writerId);
            } else {
                long oldestActivityClockTimestamp = activityWALReader.oldestActivityClockTimestamp(tenantId, currentPartitionId);
                long ageOfOldestActivity = System.currentTimeMillis() - oldestActivityClockTimestamp;
                if (oldestActivityClockTimestamp > 0 && ageOfOldestActivity > partitionMaximumAgeInMillis) {
                    List<MiruPartitionedActivity> partitionedActivities = new ArrayList<>();
                    int currentLatestIndex = partitionIdProvider.getLatestIndex(tenantId, currentPartitionId, writerId);
                    partitionedActivities.add(partitionedActivityFactory.end(writerId, currentPartitionId, tenantId, currentLatestIndex));

                    MiruPartitionId nextPartitionId = currentPartitionId.next();
                    int nextLatestIndex = partitionIdProvider.getLatestIndex(tenantId, nextPartitionId, writerId);
                    partitionedActivities.add(partitionedActivityFactory.begin(writerId, nextPartitionId, tenantId, nextLatestIndex));

                    flush(tenantId, nextPartitionId, true, partitionedActivities);
                }
            }
        }
    }

    public List<MiruPartitionedActivity> writeActivities(MiruTenantId tenantId, List<MiruActivity> activities, boolean recoverFromRemoval)
        throws Exception {

        MiruPartitionId currentPartition;
        List<MiruPartitionedActivity> partitionedActivities;
        boolean partitionRolloverOccurred = false;

        synchronized (locks.lock(tenantId)) {
            MiruPartitionCursor partitionCursor = partitionIdProvider.getCursor(tenantId, writerId);
            PartitionedLists partitionedLists = partition(tenantId, activities, partitionCursor, recoverFromRemoval);
            partitionIdProvider.saveCursor(tenantId, partitionCursor, writerId);

            currentPartition = partitionCursor.getPartitionId();

            // by always writing a "begin" we ensure the writer's index is always written to the WAL (used to reset the cursor on restart)
            Set<MiruPartitionId> begins = new HashSet<>();
            Set<MiruPartitionId> ends = new HashSet<>();
            begins.add(currentPartition);

            for (MiruPartitionedActivity partitionedActivity : partitionedLists.activities) {
                int comparison = currentPartition.compareTo(partitionedActivity.partitionId);
                if (comparison < 0) {
                    ends.add(currentPartition);
                    partitionRolloverOccurred = true;
                    currentPartition = partitionedActivity.partitionId;
                    begins.add(currentPartition);
                } else if (comparison > 0) {
                    throw new RuntimeException("Should be impossible!");
                }
            }

            int capacity = partitionedLists.activities.size() + partitionedLists.repairs.size() + begins.size() + ends.size();
            partitionedActivities = Lists.newArrayListWithCapacity(capacity);
            partitionedActivities.addAll(partitionedLists.activities);

            for (MiruPartitionId partition : begins) {
                int latestIndex = partitionIdProvider.getLatestIndex(tenantId, partition, writerId);
                partitionedActivities.add(partitionedActivityFactory.begin(writerId, partition, tenantId, latestIndex));
            }
            for (MiruPartitionId partition : ends) {
                int latestIndex = partitionIdProvider.getLatestIndex(tenantId, partition, writerId);
                partitionedActivities.add(partitionedActivityFactory.end(writerId, partition, tenantId, latestIndex));
            }

            partitionedActivities.addAll(partitionedLists.repairs);
        }

        flush(tenantId, currentPartition, partitionRolloverOccurred, partitionedActivities);

        return partitionedActivities;
    }

    public void removeActivities(MiruTenantId tenantId, List<MiruActivity> activities) throws Exception {
        synchronized (locks.lock(tenantId)) {
            List<MiruPartitionedActivity> partitionedActivities = new ArrayList<>();

            Long[] times = new Long[activities.size()];
            int index = 0;
            for (MiruActivity activity : activities) {
                times[index] = activity.time;
                index++;
            }

            MiruVersionedActivityLookupEntry[] versionedEntries = walLookup.getVersionedEntries(tenantId, times);
            index = 0;
            for (MiruActivity activity : activities) {
                MiruVersionedActivityLookupEntry versionedEntry = versionedEntries[index];
                index++;
                if (versionedEntry != null) {
                    if (activity.version > versionedEntry.version) {
                        MiruPartitionId partitionId = MiruPartitionId.of(versionedEntry.entry.partitionId);
                        partitionedActivities.add(partitionedActivityFactory.remove(writerId, partitionId, versionedEntry.entry.index, activity));
                    } else {
                        log.debug("Ignored stale deletion {} <= {} of activity {}", activity.version, versionedEntry.version, activity);
                    }
                } else {
                    log.debug("Ignored removal of nonexistent activity {}", activity);
                }
            }

            activityWALWriter.write(tenantId, partitionedActivities);
            walLookup.add(tenantId, partitionedActivities);
        }
    }

    public void writeReadEvent(MiruTenantId tenantId, MiruReadEvent readEvent) throws Exception {
        synchronized (locks.lock(tenantId)) {
            MiruPartitionCursor partitionCursor = partitionIdProvider.getCursor(tenantId, writerId);
            MiruPartitionId latestPartitionId = partitionCursor.getPartitionId();
            int lastIndex = partitionCursor.last();

            MiruPartitionedActivity currentActivity = partitionedActivityFactory.read(writerId, latestPartitionId, lastIndex, readEvent);
            readTrackingWAL.write(tenantId, Collections.singletonList(currentActivity));
        }
    }

    public void writeUnreadEvent(MiruTenantId tenantId, MiruReadEvent readEvent) throws Exception {
        synchronized (locks.lock(tenantId)) {
            MiruPartitionCursor partitionCursor = partitionIdProvider.getCursor(tenantId, writerId);
            MiruPartitionId latestPartitionId = partitionCursor.getPartitionId();
            int lastIndex = partitionCursor.last();

            MiruPartitionedActivity currentActivity = partitionedActivityFactory.unread(writerId, latestPartitionId, lastIndex, readEvent);
            readTrackingWAL.write(tenantId, Collections.singletonList(currentActivity));
        }
    }

    public void writeAllReadEvent(MiruTenantId tenantId, MiruReadEvent readEvent) throws Exception {
        synchronized (locks.lock(tenantId)) {
            MiruPartitionCursor partitionCursor = partitionIdProvider.getCursor(tenantId, writerId);
            MiruPartitionId latestPartitionId = partitionCursor.getPartitionId();
            int lastIndex = partitionCursor.last();

            MiruPartitionedActivity currentActivity = partitionedActivityFactory.allread(writerId, latestPartitionId, lastIndex, readEvent);
            readTrackingWAL.write(tenantId, Collections.singletonList(currentActivity));
        }
    }

    private void flush(MiruTenantId tenantId, MiruPartitionId currentPartition, boolean partitionRolloverOccurred,
        List<MiruPartitionedActivity> partitionedActivities) throws Exception {

        activityWALWriter.write(tenantId, partitionedActivities);
        walLookup.add(tenantId, partitionedActivities);

        if (!partitionedActivities.isEmpty()) {
            log.set(ValueType.COUNT, "partitioner>index>" + currentPartition.getId(),
                partitionedActivities.get(partitionedActivities.size() - 1).index, tenantId.toString());
            log.set(ValueType.COUNT, "partitioner>partition", currentPartition.getId(), tenantId.toString());
        }

        if (partitionRolloverOccurred) {
            synchronized (locks.lock(tenantId)) {
                partitionIdProvider.setLargestPartitionIdForWriter(tenantId, currentPartition, writerId);
            }
        }
    }

    private PartitionedLists partition(MiruTenantId tenantId, List<MiruActivity> activities, MiruPartitionCursor cursor,
        boolean recoverFromRemoval) throws Exception {

        for (MiruActivity activity : activities) {
            if (!tenantId.equals(activity.tenantId)) {
                throw new RuntimeException("It is expected that the provided activities will all have the same tenant id.");
            }
        }

        List<MiruPartitionedActivity> partitionedRepairs = new ArrayList<>();
        List<MiruPartitionedActivity> partitionedActivities = new ArrayList<>();

        Long[] times = new Long[activities.size()];
        int index = 0;
        for (MiruActivity activity : activities) {
            times[index] = activity.time;
            index++;
        }

        MiruVersionedActivityLookupEntry[] versionedEntries = walLookup.getVersionedEntries(tenantId, times);
        index = 0;

        for (MiruActivity activity : activities) {
            MiruVersionedActivityLookupEntry versionedEntry = versionedEntries[index];
            index++;
            if (versionedEntry != null) {
                if (activity.version > versionedEntry.version) {
                    if (!versionedEntry.entry.removed || recoverFromRemoval) {
                        MiruPartitionId partitionId = MiruPartitionId.of(versionedEntry.entry.partitionId);
                        partitionedRepairs.add(partitionedActivityFactory.repair(writerId, partitionId, versionedEntry.entry.index, activity));
                    } else {
                        log.debug("Ignored removed activity {}", activity);
                    }
                } else {
                    log.debug("Ignored stale version {} <= {} of activity {}", activity.version, versionedEntry.version, activity);
                }
            } else {
                while (!cursor.hasNext()) {
                    cursor = partitionIdProvider.nextCursor(tenantId, cursor, writerId);
                }
                int id = cursor.next();
                partitionedActivities.add(partitionedActivityFactory.activity(writerId, cursor.getPartitionId(), id, activity));
            }
        }

        return new PartitionedLists(partitionedRepairs, partitionedActivities);
    }

    private static class PartitionedLists {

        public final List<MiruPartitionedActivity> repairs;
        public final List<MiruPartitionedActivity> activities;

        private PartitionedLists(List<MiruPartitionedActivity> repairs, List<MiruPartitionedActivity> activities) {
            this.repairs = repairs;
            this.activities = activities;
        }
    }

}
