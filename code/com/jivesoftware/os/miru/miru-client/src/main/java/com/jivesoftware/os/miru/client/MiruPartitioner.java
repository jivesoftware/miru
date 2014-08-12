package com.jivesoftware.os.miru.client;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruActivityLookupTable;
import com.jivesoftware.os.miru.cluster.rcvs.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALWriter;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

/** @author jonathan */
@Singleton
public class MiruPartitioner {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final int writerId;
    private final MiruPartitionIdProvider partitionIdProvider;
    private final MiruActivityWALWriter activityWALWriter;
    private final MiruReadTrackingWALWriter readTrackingWAL;
    private final MiruActivityLookupTable activityLookupTable;
    private final MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();
    private final StripingLocksProvider<MiruTenantId> locks = new StripingLocksProvider<>(64);

    @Inject
    public MiruPartitioner(@Named("miruWriterId") int writerId,
        MiruPartitionIdProvider partitionIdProvider,
        MiruActivityWALWriter activityWALWriter,
        MiruReadTrackingWALWriter readTrackingWAL,
        MiruActivityLookupTable activityLookupTable) {
        this.writerId = writerId;
        this.partitionIdProvider = partitionIdProvider;
        this.activityWALWriter = activityWALWriter;
        this.readTrackingWAL = readTrackingWAL;
        this.activityLookupTable = activityLookupTable;
    }

    public void checkForAlignmentWithOtherWriters(MiruTenantId tenantId) throws Exception {
        synchronized (locks.lock(tenantId)) {
            MiruPartitionId largestPartitionIdAcrossAllWriters = partitionIdProvider.getLargestPartitionIdAcrossAllWriters(tenantId);
            MiruPartitionCursor partitionCursor = partitionIdProvider.getCursor(tenantId, writerId);
            if (partitionCursor.getPartitionId().compareTo(largestPartitionIdAcrossAllWriters) < 0) {

                List<MiruPartitionedActivity> partitionedActivities = new ArrayList<>();
                for (MiruPartitionId partition = partitionCursor.getPartitionId();
                    partition.compareTo(largestPartitionIdAcrossAllWriters) < 0;
                    partition = partition.next()) {

                    int latestIndex = partitionIdProvider.getLatestIndex(tenantId, partition, writerId);
                    partitionedActivities.add(partitionedActivityFactory.begin(writerId, partition, tenantId, latestIndex));
                    partitionedActivities.add(partitionedActivityFactory.end(writerId, partition, tenantId, latestIndex));
                }

                int latestIndex = partitionIdProvider.getLatestIndex(tenantId, largestPartitionIdAcrossAllWriters, writerId);
                partitionedActivities.add(partitionedActivityFactory.begin(writerId, largestPartitionIdAcrossAllWriters, tenantId, latestIndex));

                flush(tenantId, largestPartitionIdAcrossAllWriters, true, partitionedActivities);
            }
        }
    }

    public List<MiruPartitionedActivity> writeActivities(MiruTenantId tenantId, List<MiruActivity> activities, boolean recoverFromRemoval)
        throws Exception {

        synchronized (locks.lock(tenantId)) {

            MiruPartitionCursor partitionCursor = partitionIdProvider.getCursor(tenantId, writerId);

            Set<MiruPartitionId> begins = new HashSet<>();
            Set<MiruPartitionId> ends = new HashSet<>();

            PartitionedLists partitionedLists = partition(tenantId, activities, partitionCursor, recoverFromRemoval);
            List<MiruPartitionedActivity> partitionedActivities = Lists.newArrayList(partitionedLists.activities);

            MiruPartitionId currentPartition = partitionCursor.getPartitionId();

            // by always writing a "begin" we ensure the writer's index is always written to the WAL (used to reset the cursor on restart)
            begins.add(currentPartition);

            for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
                int comparison = currentPartition.compareTo(partitionedActivity.partitionId);
                if (comparison < 0) {
                    ends.add(currentPartition);
                    currentPartition = partitionedActivity.partitionId;
                    begins.add(currentPartition);
                } else if (comparison > 0) {
                    throw new RuntimeException("Should be impossible!");
                }
            }

            for (MiruPartitionId partition : begins) {
                int latestIndex = partitionIdProvider.getLatestIndex(tenantId, partition, writerId);
                partitionedActivities.add(partitionedActivityFactory.begin(writerId, partition, tenantId, latestIndex));
            }
            for (MiruPartitionId partition : ends) {
                int latestIndex = partitionIdProvider.getLatestIndex(tenantId, partition, writerId);
                partitionedActivities.add(partitionedActivityFactory.end(writerId, partition, tenantId, latestIndex));
            }

            partitionedActivities.addAll(partitionedLists.repairs);

            flush(tenantId, currentPartition, !ends.isEmpty(), partitionedActivities);

            return partitionedActivities;
        }
    }

    public void removeActivities(MiruTenantId tenantId, List<MiruActivity> activities) throws Exception {
        synchronized (locks.lock(tenantId)) {
            List<MiruPartitionedActivity> partitionedActivities = new ArrayList<>();

            for (MiruActivity activity : activities) {
                MiruVersionedActivityLookupEntry versionedEntry = activityLookupTable.getVersionedEntry(tenantId, activity.time);
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
            activityLookupTable.add(tenantId, partitionedActivities);
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
        activityLookupTable.add(tenantId, partitionedActivities);

        if (partitionRolloverOccurred) {
            partitionIdProvider.setLargestPartitionIdForWriter(tenantId, currentPartition, writerId);
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

        for (MiruActivity activity : activities) {
            MiruVersionedActivityLookupEntry versionedEntry = activityLookupTable.getVersionedEntry(tenantId, activity.time);
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
