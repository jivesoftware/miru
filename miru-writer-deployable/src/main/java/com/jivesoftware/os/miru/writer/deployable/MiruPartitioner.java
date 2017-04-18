package com.jivesoftware.os.miru.writer.deployable;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.api.wal.MiruActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.writer.partition.MiruPartitionCursor;
import com.jivesoftware.os.miru.writer.partition.MiruPartitionIdProvider;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** @author jonathan */
public class MiruPartitioner {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final int writerId;
    private final MiruPartitionIdProvider partitionIdProvider;
    private final MiruWALClient<?, ?> walClient;
    private final MiruClusterClient clusterClient;
    private final long partitionMaximumAgeInMillis;
    private final MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();
    private final StripingLocksProvider<MiruTenantId> locks = new StripingLocksProvider<>(64);
    private final Cache<MiruTenantId, Map<MiruPartitionId, MiruClusterClient.PartitionRange>> tenantIngressRangeCache;
    private final Set<TenantAndPartition> closedTenantPartitions = Collections.newSetFromMap(Maps.newConcurrentMap());

    public MiruPartitioner(int writerId,
        MiruPartitionIdProvider partitionIdProvider,
        MiruWALClient<?, ?> walClient,
        MiruClusterClient clusterClient,
        long partitionMaximumAgeInMillis) {
        this.writerId = writerId;
        this.partitionIdProvider = partitionIdProvider;
        this.walClient = walClient;
        this.clusterClient = clusterClient;
        this.partitionMaximumAgeInMillis = partitionMaximumAgeInMillis;
        this.tenantIngressRangeCache = CacheBuilder.newBuilder()
            .maximumSize(10_000)
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build();
    }

    public void checkForAlignmentWithOtherWriters(MiruTenantId tenantId) throws Exception {
        synchronized (locks.lock(tenantId, 0)) {
            MiruPartitionId largestPartitionIdAcrossAllWriters = partitionIdProvider.getLargestPartitionIdAcrossAllWriters(tenantId);
            MiruPartitionCursor partitionCursor = partitionIdProvider.getCursor(tenantId, writerId);
            MiruPartitionId currentPartitionId = partitionCursor.getPartitionId();
            if (currentPartitionId.compareTo(largestPartitionIdAcrossAllWriters) < 0) {
                ListMultimap<MiruPartitionId, MiruPartitionedActivity> partitionedActivities = ArrayListMultimap.create();
                for (MiruPartitionId partitionId = currentPartitionId;
                     partitionId.compareTo(largestPartitionIdAcrossAllWriters) < 0;
                     partitionId = partitionId.next()) {

                    int latestIndex = partitionIdProvider.getLatestIndex(tenantId, partitionId, writerId);
                    partitionedActivities.put(partitionId, partitionedActivityFactory.begin(writerId, partitionId, tenantId, latestIndex));
                    partitionedActivities.put(partitionId, partitionedActivityFactory.end(writerId, partitionId, tenantId, latestIndex));
                }

                int latestIndex = partitionIdProvider.getLatestIndex(tenantId, largestPartitionIdAcrossAllWriters, writerId);
                partitionedActivities.put(largestPartitionIdAcrossAllWriters,
                    partitionedActivityFactory.begin(writerId, largestPartitionIdAcrossAllWriters, tenantId, latestIndex));

                log.info("Cursor for {} is out of alignment: {}", tenantId, partitionCursor);
                flushActivities(tenantId, largestPartitionIdAcrossAllWriters, latestIndex, true,
                    new PartitionedLists(Collections.emptyList(), partitionedActivities));
            } else {
                long oldestActivityClockTimestamp = walClient.oldestActivityClockTimestamp(tenantId, currentPartitionId); //TODO cache
                long ageOfOldestActivity = System.currentTimeMillis() - oldestActivityClockTimestamp;
                if (oldestActivityClockTimestamp > 0 && ageOfOldestActivity > partitionMaximumAgeInMillis) {
                    ListMultimap<MiruPartitionId, MiruPartitionedActivity> partitionedActivities = ArrayListMultimap.create();
                    int currentLatestIndex = partitionIdProvider.getLatestIndex(tenantId, currentPartitionId, writerId);
                    partitionedActivities.put(currentPartitionId, partitionedActivityFactory.end(writerId, currentPartitionId, tenantId, currentLatestIndex));

                    MiruPartitionId nextPartitionId = currentPartitionId.next();
                    int nextLatestIndex = partitionIdProvider.getLatestIndex(tenantId, nextPartitionId, writerId);
                    partitionedActivities.put(nextPartitionId, partitionedActivityFactory.begin(writerId, nextPartitionId, tenantId, nextLatestIndex));

                    flushActivities(tenantId, nextPartitionId, nextLatestIndex, true, new PartitionedLists(Collections.emptyList(), partitionedActivities));
                }
            }
        }
    }

    public void writeActivities(MiruTenantId tenantId, List<MiruActivity> activities, boolean recoverFromRemoval)
        throws Exception {

        List<List<MiruActivity>> partitions = Lists.partition(activities, 10_000); //TODO config
        for (List<MiruActivity> partition : partitions) {
            MiruPartitionId latestPartitionId;
            int latestIndex;
            int indexAdvanced;
            boolean partitionRolloverOccurred = false;
            PartitionedLists partitionedLists;
            TenantAndPartition end = null;

            synchronized (locks.lock(tenantId, 0)) {
                MiruPartitionCursor partitionCursor = partitionIdProvider.getCursor(tenantId, writerId);
                if (partitionCursor.isMaxCapacity()) {
                    log.info("Cursor for {} is at max capacity: {}", tenantId, partitionCursor);
                    end = new TenantAndPartition(tenantId, partitionCursor.getPartitionId());
                    partitionCursor = partitionIdProvider.nextCursor(tenantId, partitionCursor, writerId);
                    partitionRolloverOccurred = true;
                } else {
                    MiruPartitionId endPartitionId = partitionCursor.getPartitionId().prev();
                    if (endPartitionId != null) {
                        end = new TenantAndPartition(tenantId, endPartitionId);
                    }
                }

                int indexBefore = partitionCursor.last();
                partitionedLists = partition(tenantId, partition, partitionCursor, recoverFromRemoval);
                if (!partitionRolloverOccurred && partitionedLists.activities.isEmpty() && partitionedLists.repairs.isEmpty()) {
                    continue;
                }

                latestPartitionId = partitionCursor.getPartitionId();
                latestIndex = partitionCursor.last();
                indexAdvanced = partitionCursor.last() - indexBefore;

                partitionIdProvider.saveCursor(tenantId, partitionCursor, writerId);
            }

            partitionedLists.activities.add(partitionedActivityFactory.begin(writerId, latestPartitionId, tenantId, latestIndex));

            if (end != null && !closedTenantPartitions.contains(end)) {
                MiruPartitionId endPartitionId = end.partitionId;
                int endLatestIndex = partitionIdProvider.getLatestIndex(tenantId, endPartitionId, writerId);
                partitionedLists.repairs.put(endPartitionId, partitionedActivityFactory.end(writerId, endPartitionId, tenantId, endLatestIndex));
            }

            try {
                flushActivities(tenantId, latestPartitionId, latestIndex, partitionRolloverOccurred, partitionedLists);
                if (end != null) {
                    closedTenantPartitions.add(end);
                }
            } catch (Exception e) {
                synchronized (locks.lock(tenantId, 0)) {
                    partitionIdProvider.rewindCursor(tenantId, writerId, indexAdvanced);
                }
                throw e;
            }
        }
    }

    public void removeActivities(MiruTenantId tenantId, List<MiruActivity> activities) throws Exception {
        synchronized (locks.lock(tenantId, 0)) {
            ListMultimap<MiruPartitionId, MiruPartitionedActivity> partitionedActivities = ArrayListMultimap.create();
            List<MiruVersionedActivityLookupEntry> versionedEntries = Lists.newArrayList();

            Long[] times = new Long[activities.size()];
            int index = 0;
            for (MiruActivity activity : activities) {
                times[index] = activity.time;
                index++;
            }

            List<MiruVersionedActivityLookupEntry> lookupEntries = getVersionedEntries(tenantId, times);
            index = 0;
            for (MiruActivity activity : activities) {
                MiruVersionedActivityLookupEntry versionedEntry = lookupEntries.get(index);
                index++;
                if (versionedEntry != null) {
                    if (activity.version > versionedEntry.version) {
                        MiruPartitionId partitionId = MiruPartitionId.of(versionedEntry.entry.partitionId);
                        partitionedActivities.put(partitionId, partitionedActivityFactory.remove(writerId, partitionId, versionedEntry.entry.index, activity));
                        versionedEntries.add(new MiruVersionedActivityLookupEntry(activity.time, activity.version,
                            new MiruActivityLookupEntry(partitionId.getId(), versionedEntry.entry.index, versionedEntry.entry.writerId, true)));
                    } else {
                        log.debug("Ignored stale deletion {} <= {} of activity {}", activity.version, versionedEntry.version, activity);
                    }
                } else {
                    log.debug("Ignored removal of nonexistent activity {}", activity);
                }
            }

            for (MiruPartitionId partitionId : partitionedActivities.keySet()) {
                walClient.writeActivity(tenantId, partitionId, partitionedActivities.get(partitionId));
            }
        }
    }

    public void writeReadEvents(MiruTenantId tenantId, List<MiruReadEvent> readEvents) throws Exception {
        synchronized (locks.lock(tenantId, 0)) {
            walClient.writeReadTracking(tenantId,
                readEvents,
                readEvent -> partitionedActivityFactory.read(writerId, MiruPartitionId.of(-1), -1, readEvent));
        }
    }

    public void writeUnreadEvents(MiruTenantId tenantId, List<MiruReadEvent> readEvents) throws Exception {
        synchronized (locks.lock(tenantId, 0)) {
            walClient.writeReadTracking(tenantId,
                readEvents,
                readEvent -> partitionedActivityFactory.unread(writerId, MiruPartitionId.of(-1), -1, readEvent));
        }
    }

    public void writeAllReadEvents(MiruTenantId tenantId, List<MiruReadEvent> readEvents) throws Exception {
        synchronized (locks.lock(tenantId, 0)) {
            walClient.writeReadTracking(tenantId,
                readEvents,
                readEvent -> partitionedActivityFactory.allread(writerId, MiruPartitionId.of(-1), -1, readEvent));
        }
    }

    public void updateCursor(int writerId, MiruTenantId tenantId, MiruPartitionId partitionId, int index) throws Exception {
        partitionIdProvider.setLargestPartitionIdForWriter(tenantId, partitionId, writerId, index);
    }

    private void flushActivities(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        int latestIndex,
        boolean partitionRolloverOccurred,
        PartitionedLists partitionedLists) throws Exception {

        if (!partitionedLists.activities.isEmpty()) {
            walClient.writeActivity(tenantId, partitionId, partitionedLists.activities);
            log.set(ValueType.COUNT, "partitioner>index>" + partitionId.getId(), latestIndex, tenantId.toString());
            log.set(ValueType.COUNT, "partitioner>partition", partitionId.getId(), tenantId.toString());
        }

        for (MiruPartitionId repairPartitionId : partitionedLists.repairs.keySet()) {
            List<MiruPartitionedActivity> repairActivities = partitionedLists.repairs.get(repairPartitionId);
            if (!repairActivities.isEmpty()) {
                walClient.writeActivity(tenantId, repairPartitionId, repairActivities);
                log.inc("partitioner>repair>calls>" + repairPartitionId.getId(), tenantId.toString());
                log.inc("partitioner>repair>count>" + repairPartitionId.getId(), repairActivities.size(), tenantId.toString());
            }
        }

        if (partitionRolloverOccurred) {
            synchronized (locks.lock(tenantId, 0)) {
                partitionIdProvider.setLargestPartitionIdForWriter(tenantId, partitionId, writerId, 0);
                tenantIngressRangeCache.invalidate(tenantId);
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

        ListMultimap<MiruPartitionId, MiruPartitionedActivity> partitionedRepairs = ArrayListMultimap.create();
        List<MiruPartitionedActivity> partitionedActivities = Lists.newArrayList();

        Long[] times = new Long[activities.size()];
        int index = 0;
        for (MiruActivity activity : activities) {
            times[index] = activity.time;
            index++;
        }

        List<MiruVersionedActivityLookupEntry> versionedEntries = getVersionedEntries(tenantId, times);
        index = 0;

        for (MiruActivity activity : activities) {
            MiruVersionedActivityLookupEntry versionedEntry = versionedEntries.get(index);
            index++;
            if (versionedEntry != null) {
                if (activity.version > versionedEntry.version) {
                    if (!versionedEntry.entry.removed || recoverFromRemoval) {
                        MiruPartitionId partitionId = MiruPartitionId.of(versionedEntry.entry.partitionId);
                        partitionedRepairs.put(partitionId, partitionedActivityFactory.repair(writerId, partitionId, versionedEntry.entry.index, activity));
                    } else {
                        log.debug("Ignored removed activity {}", activity);
                    }
                } else {
                    log.debug("Ignored stale version {} <= {} of activity {}", activity.version, versionedEntry.version, activity);
                }
            } else {
                int id = cursor.next();
                MiruPartitionId partitionId = cursor.getPartitionId();
                partitionedActivities.add(partitionedActivityFactory.activity(writerId, partitionId, id, activity));
            }
        }

        return new PartitionedLists(partitionedActivities, partitionedRepairs);
    }

    private List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, Long[] times) throws Exception {
        List<MiruPartitionId> partitionIds = Lists.newArrayList();
        Map<MiruPartitionId, MiruClusterClient.PartitionRange> ingressRanges = tenantIngressRangeCache.get(tenantId, () -> {
            List<MiruClusterClient.PartitionRange> partitionRanges = clusterClient.getIngressRanges(tenantId);
            if (partitionRanges == null) {
                throw new IllegalStateException("Partition ranges not available");
            }
            Map<MiruPartitionId, MiruClusterClient.PartitionRange> result = Maps.newHashMapWithExpectedSize(partitionRanges.size());
            for (MiruClusterClient.PartitionRange partitionRange : partitionRanges) {
                result.put(partitionRange.partitionId, partitionRange);
            }
            return result;
        });

        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;
        for (Long time : times) {
            minTime = Math.min(minTime, time);
            maxTime = Math.max(maxTime, time);
        }

        for (Map.Entry<MiruPartitionId, MiruClusterClient.PartitionRange> entry : ingressRanges.entrySet()) {
            RangeMinMax range = entry.getValue().rangeMinMax;
            long destroyAfterTimestamp = entry.getValue().destroyAfterTimestamp;
            boolean destroyed = destroyAfterTimestamp > 0 && System.currentTimeMillis() > destroyAfterTimestamp;
            if (!destroyed && range.orderIdMin != -1 && range.orderIdMax != -1 && range.orderIdMin <= maxTime && range.orderIdMax >= minTime) {
                partitionIds.add(entry.getKey());
            }
        }
        Collections.sort(partitionIds, Collections.reverseOrder());

        List<MiruVersionedActivityLookupEntry> result = Arrays.asList(new MiruVersionedActivityLookupEntry[times.length]);
        int found = 0;
        for (MiruPartitionId partitionId : partitionIds) {
            List<MiruVersionedActivityLookupEntry> versionedEntries = walClient.getVersionedEntries(tenantId, partitionId, times);
            if (versionedEntries == null) {
                throw new IllegalStateException("Unable to perform versioned entries lookup because WAL is unavailable for " + tenantId + " " + partitionId);
            }
            for (int i = 0; i < versionedEntries.size(); i++) {
                MiruVersionedActivityLookupEntry versionedEntry = versionedEntries.get(i);
                if (versionedEntry != null) {
                    result.set(i, versionedEntry);
                    times[i] = null;
                    found++;
                    if (found == times.length) {
                        return result;
                    }
                }
            }
        }
        return result;
    }

    private static class PartitionedLists {

        public final List<MiruPartitionedActivity> activities;
        public final ListMultimap<MiruPartitionId, MiruPartitionedActivity> repairs;

        public PartitionedLists(List<MiruPartitionedActivity> activities,
            ListMultimap<MiruPartitionId, MiruPartitionedActivity> repairs) {
            this.activities = activities;
            this.repairs = repairs;
        }
    }

}
