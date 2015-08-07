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
import java.util.HashSet;
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
    private final Cache<MiruTenantId, Map<MiruPartitionId, RangeMinMax>> tenantIngressRangeCache;

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

                flush(tenantId, largestPartitionIdAcrossAllWriters, latestIndex, true, partitionedActivities);
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

                    flush(tenantId, nextPartitionId, nextLatestIndex, true, partitionedActivities);
                }
            }
        }
    }

    public void writeActivities(MiruTenantId tenantId, List<MiruActivity> activities, boolean recoverFromRemoval)
        throws Exception {

        MiruPartitionId latestPartitionId;
        int latestIndex;
        ListMultimap<MiruPartitionId, MiruPartitionedActivity> partitionedActivities;
        boolean partitionRolloverOccurred = false;

        //TODO probably much more efficient just to overflow the latest partition and then roll the cursor
        synchronized (locks.lock(tenantId, 0)) {
            MiruPartitionCursor partitionCursor = partitionIdProvider.getCursor(tenantId, writerId);
            PartitionedLists partitionedLists = partition(tenantId, activities, partitionCursor, recoverFromRemoval);
            partitionIdProvider.saveCursor(tenantId, partitionCursor, writerId);

            latestPartitionId = partitionCursor.getPartitionId();
            latestIndex = partitionCursor.last();

            // by always writing a "begin" we ensure the writer's index is always written to the WAL (used to reset the cursor on restart)
            Set<MiruPartitionId> begins = new HashSet<>();
            Set<MiruPartitionId> ends = new HashSet<>();
            begins.add(latestPartitionId);

            for (MiruPartitionId partitionId : partitionedLists.activities.keySet()) {
                int comparison = latestPartitionId.compareTo(partitionId);
                if (comparison < 0) {
                    ends.add(latestPartitionId);
                    partitionRolloverOccurred = true;
                    latestPartitionId = partitionId;
                    begins.add(latestPartitionId);
                } else if (comparison > 0) {
                    throw new RuntimeException("Should be impossible!");
                }
            }

            partitionedActivities = ArrayListMultimap.create();
            partitionedActivities.putAll(partitionedLists.activities);

            for (MiruPartitionId partitionId : begins) {
                int partitionLatestIndex = partitionIdProvider.getLatestIndex(tenantId, partitionId, writerId);
                partitionedActivities.put(partitionId, partitionedActivityFactory.begin(writerId, partitionId, tenantId, partitionLatestIndex));
            }
            for (MiruPartitionId partitionId : ends) {
                int partitionLatestIndex = partitionIdProvider.getLatestIndex(tenantId, partitionId, writerId);
                partitionedActivities.put(partitionId, partitionedActivityFactory.end(writerId, partitionId, tenantId, partitionLatestIndex));
            }

            partitionedActivities.putAll(partitionedLists.repairs);
        }

        flush(tenantId, latestPartitionId, latestIndex, partitionRolloverOccurred, partitionedActivities);
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

    public void writeReadEvent(MiruTenantId tenantId, MiruReadEvent readEvent) throws Exception {
        synchronized (locks.lock(tenantId, 0)) {
            //TODO this is dumb, split readEvents out of activityFactory
            MiruPartitionedActivity currentActivity = partitionedActivityFactory.read(writerId, MiruPartitionId.of(-1), -1, readEvent);
            walClient.writeReadTracking(tenantId, readEvent.streamId, Collections.singletonList(currentActivity));
        }
    }

    public void writeUnreadEvent(MiruTenantId tenantId, MiruReadEvent readEvent) throws Exception {
        synchronized (locks.lock(tenantId, 0)) {
            //TODO this is dumb, split readEvents out of activityFactory
            MiruPartitionedActivity currentActivity = partitionedActivityFactory.unread(writerId, MiruPartitionId.of(-1), -1, readEvent);
            walClient.writeReadTracking(tenantId, readEvent.streamId, Collections.singletonList(currentActivity));
        }
    }

    public void writeAllReadEvent(MiruTenantId tenantId, MiruReadEvent readEvent) throws Exception {
        synchronized (locks.lock(tenantId, 0)) {
            //TODO this is dumb, split readEvents out of activityFactory
            MiruPartitionedActivity currentActivity = partitionedActivityFactory.allread(writerId, MiruPartitionId.of(-1), -1, readEvent);
            walClient.writeReadTracking(tenantId, readEvent.streamId, Collections.singletonList(currentActivity));
        }
    }

    private void flush(MiruTenantId tenantId,
        MiruPartitionId latestPartitionId,
        int latestIndex,
        boolean partitionRolloverOccurred,
        ListMultimap<MiruPartitionId, MiruPartitionedActivity> partitionedActivities) throws Exception {

        for (MiruPartitionId partitionId : partitionedActivities.keySet()) {
            walClient.writeActivity(tenantId, partitionId, partitionedActivities.get(partitionId));
        }

        if (!partitionedActivities.isEmpty()) {
            log.set(ValueType.COUNT, "partitioner>index>" + latestPartitionId.getId(), latestIndex, tenantId.toString());
            log.set(ValueType.COUNT, "partitioner>partition", latestPartitionId.getId(), tenantId.toString());
        }

        if (partitionRolloverOccurred) {
            synchronized (locks.lock(tenantId, 0)) {
                partitionIdProvider.setLargestPartitionIdForWriter(tenantId, latestPartitionId, writerId);
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
        ListMultimap<MiruPartitionId, MiruPartitionedActivity> partitionedActivities = ArrayListMultimap.create();

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
                while (!cursor.hasNext()) {
                    cursor = partitionIdProvider.nextCursor(tenantId, cursor, writerId);
                }
                int id = cursor.next();
                MiruPartitionId partitionId = cursor.getPartitionId();
                partitionedActivities.put(partitionId, partitionedActivityFactory.activity(writerId, partitionId, id, activity));
            }
        }

        return new PartitionedLists(partitionedRepairs, partitionedActivities);
    }

    private List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, Long[] times) throws Exception {
        List<MiruPartitionId> partitionIds = Lists.newArrayList();
        Map<MiruPartitionId, RangeMinMax> ingressRanges = tenantIngressRangeCache.get(tenantId, () -> {
            List<MiruClusterClient.PartitionRange> partitionRanges = clusterClient.getIngressRanges(tenantId);
            if (partitionRanges == null) {
                throw new IllegalStateException("Partition ranges not available");
            }
            Map<MiruPartitionId, RangeMinMax> result = Maps.newHashMap();
            for (MiruClusterClient.PartitionRange partitionRange : partitionRanges) {
                result.put(partitionRange.partitionId, partitionRange.rangeMinMax);
            }
            return result;
        });

        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;
        for (Long time : times) {
            minTime = Math.min(minTime, time);
            maxTime = Math.max(maxTime, time);
        }

        for (Map.Entry<MiruPartitionId, RangeMinMax> entry : ingressRanges.entrySet()) {
            RangeMinMax range = entry.getValue();
            if (range.orderIdMin != -1 && range.orderIdMax != -1 && range.orderIdMin <= maxTime && range.orderIdMax >= minTime) {
                partitionIds.add(entry.getKey());
            }
        }
        Collections.sort(partitionIds, Collections.reverseOrder());

        List<MiruVersionedActivityLookupEntry> result = Arrays.asList(new MiruVersionedActivityLookupEntry[times.length]);
        int found = 0;
        for (MiruPartitionId partitionId : partitionIds) {
            List<MiruVersionedActivityLookupEntry> versionedEntries = walClient.getVersionedEntries(tenantId, partitionId, times);
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

        public final ListMultimap<MiruPartitionId, MiruPartitionedActivity> repairs;
        public final ListMultimap<MiruPartitionId, MiruPartitionedActivity> activities;

        private PartitionedLists(ListMultimap<MiruPartitionId, MiruPartitionedActivity> repairs,
            ListMultimap<MiruPartitionId, MiruPartitionedActivity> activities) {
            this.repairs = repairs;
            this.activities = activities;
        }
    }

}
