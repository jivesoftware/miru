package com.jivesoftware.os.miru.wal;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruIngressUpdate;
import com.jivesoftware.os.miru.api.topology.MiruPartitionStatus;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.lookup.MiruWALLookup;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALWriter;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MiruWALDirector<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> implements MiruWALClient<C, S> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruWALLookup walLookup;
    private final MiruActivityWALReader<C, S> activityWALReader;
    private final MiruActivityWALWriter activityWALWriter;
    private final MiruReadTrackingWALReader<C, S> readTrackingWALReader;
    private final MiruReadTrackingWALWriter readTrackingWALWriter;
    private final MiruClusterClient clusterClient;

    private final MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();
    private final Callable<Void> repairLookupCallback = () -> {
        repairLookup();
        return null;
    };

    public MiruWALDirector(MiruWALLookup walLookup,
        MiruActivityWALReader<C, S> activityWALReader,
        MiruActivityWALWriter activityWALWriter,
        MiruReadTrackingWALReader<C, S> readTrackingWALReader,
        MiruReadTrackingWALWriter readTrackingWALWriter,
        MiruClusterClient clusterClient) {
        this.walLookup = walLookup;
        this.activityWALReader = activityWALReader;
        this.activityWALWriter = activityWALWriter;
        this.readTrackingWALReader = readTrackingWALReader;
        this.readTrackingWALWriter = readTrackingWALWriter;
        this.clusterClient = clusterClient;
    }

    public void repairBoundaries() throws Exception {
        List<MiruTenantId> tenantIds = getAllTenantIds();
        for (MiruTenantId tenantId : tenantIds) {
            MiruPartitionId latestPartitionId = getLargestPartitionId(tenantId);
            if (latestPartitionId != null) {
                for (MiruPartitionId partitionId = latestPartitionId.prev(); partitionId != null; partitionId = partitionId.prev()) {
                    MiruActivityWALStatus status = activityWALReader.getStatus(tenantId, partitionId);
                    if (!status.begins.equals(status.ends)) {
                        for (int begin : status.begins) {
                            if (!status.ends.contains(begin)) {
                                activityWALWriter.write(tenantId, partitionId, Arrays.asList(partitionedActivityFactory.end(begin, partitionId, tenantId, -1)));
                                LOG.info("Added missing 'end' to WAL for {} {}", tenantId, partitionId);
                            }
                        }
                        for (int end : status.ends) {
                            if (!status.begins.contains(end)) {
                                activityWALWriter.write(tenantId, partitionId, Arrays.asList(partitionedActivityFactory.begin(end, partitionId, tenantId, -1)));
                                LOG.info("Added missing 'begin' to WAL for {} {}", tenantId, partitionId);
                            }
                        }
                    }
                }
            }
        }
    }

    public void repairRanges(boolean fast) throws Exception {
        List<MiruTenantId> tenantIds = getAllTenantIds();
        for (MiruTenantId tenantId : tenantIds) {
            final Set<MiruPartitionId> found = Sets.newHashSet();
            final Set<MiruPartitionId> broken = Sets.newHashSet();
            /*TODO ask clusterClient for partitionIds with broken/missing ranges
            walLookup.streamRanges(tenantId, null, (partitionId, type, timestamp) -> {
                found.add(partitionId);
                if (type == MiruWALLookup.RangeType.orderIdMax && timestamp == Long.MAX_VALUE) {
                    broken.add(partitionId);
                }
                return true;
            });
            */

            int count = 0;
            MiruPartitionId latestPartitionId = getLargestPartitionId(tenantId);
            for (MiruPartitionId checkPartitionId = latestPartitionId; checkPartitionId != null; checkPartitionId = checkPartitionId.prev()) {
                if (!found.contains(checkPartitionId) || broken.contains(checkPartitionId)) {
                    count++;
                    if (fast) {
                        long clockMax = activityWALReader.clockMax(tenantId, checkPartitionId);
                        if (clockMax > 0) {
                            RangeMinMax minMax = new RangeMinMax();
                            minMax.clockMax = clockMax;
                            clusterClient.updateIngress(new MiruIngressUpdate(tenantId, checkPartitionId, minMax, -1, false));
                        }
                    } else {
                        RangeMinMax minMax = new RangeMinMax();
                        activityWALReader.stream(tenantId, checkPartitionId, null, 1_000,
                            (collisionId, partitionedActivity, timestamp) -> {
                                if (partitionedActivity.type.isActivityType()) {
                                    minMax.put(partitionedActivity.clockTimestamp, partitionedActivity.timestamp);
                                }
                                return true;
                            });
                        clusterClient.updateIngress(new MiruIngressUpdate(tenantId, checkPartitionId, minMax, -1, true));
                    }
                }
            }

            LOG.info("Repaired ranges in {} partitions for tenant {}", count, tenantId);
        }
    }

    public void removeDestroyed() throws Exception {
        LOG.info("Beginning scan for destroyed partitions");
        List<MiruTenantId> tenantIds = getAllTenantIds();
        for (MiruTenantId tenantId : tenantIds) {
            List<MiruPartitionStatus> status = getAllPartitionStatus(tenantId);
            int count = 0;
            for (MiruPartitionStatus partitionStatus : status) {
                if (partitionStatus.getDestroyAfterTimestamp() > 0 && System.currentTimeMillis() > partitionStatus.getDestroyAfterTimestamp()) {
                    removePartition(tenantId, partitionStatus.getPartitionId());
                    count++;
                }
            }
            if (count > 0) {
                LOG.info("Removed {} partitions for tenant {}", count, tenantId);
            }
        }
        LOG.info("Finished scan for destroyed partitions");
    }

    public List<MiruPartitionStatus> getAllPartitionStatus(MiruTenantId tenantId) throws Exception {
        MiruPartitionId largestPartitionId = getLargestPartitionId(tenantId);
        return clusterClient.getPartitionStatus(tenantId, largestPartitionId);
    }

    public void removePartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        activityWALWriter.removePartition(tenantId, partitionId);
        clusterClient.removeIngress(tenantId, partitionId);
    }

    private long packTimestamp(long millisIntoTheFuture) {
        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        long epochTime = System.currentTimeMillis() + millisIntoTheFuture - JiveEpochTimestampProvider.JIVE_EPOCH;
        return snowflakeIdPacker.pack(epochTime, 0, 0);
    }

    public void sanitizeActivityWAL(final MiruTenantId tenantId, final MiruPartitionId partitionId) throws Exception {
        long afterTimestamp = packTimestamp(TimeUnit.DAYS.toMillis(1));
        final List<MiruActivityWALColumnKey> badKeys = Lists.newArrayList();
        activityWALReader.stream(tenantId, partitionId, null, 10_000, (collisionId, partitionedActivity, timestamp) -> {
            if (partitionedActivity.timestamp > afterTimestamp && partitionedActivity.type.isActivityType()) {
                LOG.warn("Sanitizer is removing activity " + collisionId + "/" + partitionedActivity.timestamp + " from "
                    + tenantId + "/" + partitionId);
                badKeys.add(new MiruActivityWALColumnKey(partitionedActivity.type.getSort(), collisionId));
            }
            return partitionedActivity.type.isActivityType();
        });
        activityWALWriter.delete(tenantId, partitionId, badKeys);
    }

    public void sanitizeActivitySipWAL(final MiruTenantId tenantId, final MiruPartitionId partitionId) throws Exception {
        final long afterTimestamp = packTimestamp(TimeUnit.DAYS.toMillis(1));
        final List<MiruActivitySipWALColumnKey> badKeys = Lists.newArrayList();
        activityWALReader.streamSip(tenantId, partitionId, null, null, 10_000,
            (collisionId, partitionedActivity, timestamp) -> {
                if (partitionedActivity.timestamp > afterTimestamp && partitionedActivity.type.isActivityType()) {
                    LOG.warn("Sanitizer is removing sip activity " + collisionId + "/" + partitionedActivity.timestamp + " from "
                        + tenantId + "/" + partitionId);
                    badKeys.add(new MiruActivitySipWALColumnKey(partitionedActivity.type.getSort(), collisionId, timestamp));
                }
                return partitionedActivity.type.isActivityType();
            }, null);
        activityWALWriter.deleteSip(tenantId, partitionId, badKeys);
    }

    private void repairLookup() throws Exception {
        LOG.info("Repairing lookup...");
        int[] count = new int[1];
        activityWALReader.allPartitions((tenantId, partitionId) -> {
            walLookup.add(tenantId, partitionId);
            count[0]++;
            return true;
        });
        walLookup.markRepaired();
        LOG.info("Finished repairing lookup for {} partitions", count[0]);
    }

    @Override
    public HostPort[] getTenantRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId) throws Exception {
        throw new IllegalArgumentException("Type does not have tenant routing: " + routingGroupType.name());
    }

    @Override
    public HostPort[] getTenantPartitionRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        if (routingGroupType == RoutingGroupType.activity) {
            return activityWALReader.getRoutingGroup(tenantId, partitionId);
        } else {
            throw new IllegalArgumentException("Type does not have tenant-partition routing: " + routingGroupType.name());
        }
    }

    @Override
    public HostPort[] getTenantStreamRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId, MiruStreamId streamId) throws Exception {
        if (routingGroupType == RoutingGroupType.readTracking) {
            return readTrackingWALReader.getRoutingGroup(tenantId, streamId);
        } else {
            throw new IllegalArgumentException("Type does not have tenant-stream routing: " + routingGroupType.name());
        }
    }

    @Override
    public List<MiruTenantId> getAllTenantIds() throws Exception {
        return walLookup.allTenantIds(repairLookupCallback);
    }

    @Override
    public void writeActivity(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        RangeMinMax partitionMinMax = activityWALWriter.write(tenantId, partitionId, partitionedActivities);
        walLookup.add(tenantId, partitionId);
        clusterClient.updateIngress(new MiruIngressUpdate(tenantId, partitionId, partitionMinMax, System.currentTimeMillis(), false));
    }

    @Override
    public void writeReadTracking(MiruTenantId tenantId, MiruStreamId streamId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        readTrackingWALWriter.write(tenantId, streamId, partitionedActivities);
    }

    @Override
    public MiruPartitionId getLargestPartitionId(MiruTenantId tenantId) throws Exception {
        return walLookup.largestPartitionId(tenantId, repairLookupCallback);
    }

    @Override
    public WriterCursor getCursorForWriterId(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) throws Exception {
        return activityWALReader.getCursorForWriterId(tenantId, partitionId, writerId);
    }

    @Override
    public MiruActivityWALStatus getActivityWALStatusForTenant(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return activityWALReader.getStatus(tenantId, partitionId);
    }

    @Override
    public long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return activityWALReader.oldestActivityClockTimestamp(tenantId, partitionId);
    }

    @Override
    public List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, MiruPartitionId partitionId, Long[] timestamps) throws Exception {
        return activityWALReader.getVersionedEntries(tenantId, partitionId, timestamps);
    }

    @Override
    public StreamBatch<MiruWALEntry, C> getActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        C cursor,
        final int batchSize)
        throws Exception {

        List<MiruWALEntry> activities = new ArrayList<>();
        C nextCursor = activityWALReader.stream(tenantId, partitionId, cursor, batchSize,
            (collisionId, partitionedActivity, timestamp) -> {
                activities.add(new MiruWALEntry(collisionId, timestamp, partitionedActivity));
                return activities.size() < batchSize;
            });

        return new StreamBatch<>(activities, nextCursor, false, null);
    }

    @Override
    public StreamBatch<MiruWALEntry, S> sipActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        S cursor,
        Set<TimeAndVersion> lastSeen,
        final int batchSize) throws Exception {

        List<MiruWALEntry> activities = new ArrayList<>();
        Set<TimeAndVersion> suppressed = Sets.newHashSet();
        boolean[] endOfWAL = { false };
        S nextCursor = activityWALReader.streamSip(tenantId, partitionId, cursor, lastSeen, batchSize,
            (collisionId, partitionedActivity, timestamp) -> {
                if (collisionId != -1) {
                    activities.add(new MiruWALEntry(collisionId, timestamp, partitionedActivity));
                } else {
                    endOfWAL[0] = true;
                }
                return activities.size() < batchSize;
            },
            suppressed::add);

        return new StreamBatch<>(activities, nextCursor, endOfWAL[0], suppressed);
    }

    @Override
    public StreamBatch<MiruWALEntry, S> getRead(MiruTenantId tenantId,
        MiruStreamId streamId,
        S sipCursor,
        long oldestEventId,
        int batchSize) throws Exception {

        long[] minEventId = new long[1];
        int[] count = new int[1];
        S nextCursor = readTrackingWALReader.streamSip(tenantId, streamId, sipCursor, batchSize, (eventId, timestamp) -> {
            minEventId[0] = Math.min(minEventId[0], eventId);
            count[0]++;
            return count[0] < batchSize;
        });
        C cursor = readTrackingWALReader.getCursor(minEventId[0]);

        // Take either the oldest eventId or the oldest readtracking sip time
        minEventId[0] = Math.min(minEventId[0], oldestEventId);

        List<MiruWALEntry> batch = new ArrayList<>();
        readTrackingWALReader.stream(tenantId, streamId, cursor, batchSize, (collisionId, partitionedActivity, timestamp) -> {
            batch.add(new MiruWALEntry(collisionId, timestamp, partitionedActivity));
            return true; // always consume completely
        });
        return new StreamBatch<>(batch, nextCursor, batch.size() < batchSize, null);
    }

}
