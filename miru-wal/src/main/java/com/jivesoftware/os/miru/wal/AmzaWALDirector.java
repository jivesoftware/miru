package com.jivesoftware.os.miru.wal;

import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruIngressUpdate;
import com.jivesoftware.os.miru.api.topology.MiruPartitionStatus;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.api.wal.AmzaCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.RoutingGroupType;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.StreamBatch;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.WriterCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.wal.activity.amza.AmzaActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.amza.AmzaActivityWALWriter;
import com.jivesoftware.os.miru.wal.lookup.MiruWALLookup;
import com.jivesoftware.os.miru.wal.readtracking.amza.AmzaReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.amza.AmzaReadTrackingWALWriter;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 */
public class AmzaWALDirector {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruWALLookup walLookup;
    private final AmzaActivityWALReader activityWALReader;
    private final AmzaActivityWALWriter activityWALWriter;
    private final AmzaReadTrackingWALReader readTrackingWALReader;
    private final AmzaReadTrackingWALWriter readTrackingWALWriter;
    private final MiruClusterClient clusterClient;

    private final Callable<Void> repairLookupCallback = () -> {
        repairLookup();
        return null;
    };

    public AmzaWALDirector(MiruWALLookup walLookup,
        AmzaActivityWALReader activityWALReader,
        AmzaActivityWALWriter activityWALWriter,
        AmzaReadTrackingWALReader readTrackingWALReader,
        AmzaReadTrackingWALWriter readTrackingWALWriter,
        MiruClusterClient clusterClient) {
        this.walLookup = walLookup;
        this.activityWALReader = activityWALReader;
        this.activityWALWriter = activityWALWriter;
        this.readTrackingWALReader = readTrackingWALReader;
        this.readTrackingWALWriter = readTrackingWALWriter;
        this.clusterClient = clusterClient;
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
                        if (clockMax != -1) {
                            RangeMinMax minMax = new RangeMinMax();
                            minMax.clockMax = clockMax;
                            clusterClient.updateIngress(new MiruIngressUpdate(tenantId, checkPartitionId, minMax, -1, false));
                        }
                    } else {
                        RangeMinMax minMax = new RangeMinMax();
                        activityWALReader.stream(tenantId, checkPartitionId, null, 1_000, -1L,
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

    public void removeCleanup() throws Exception {
        LOG.info("Beginning scan for partitions to clean up");
        List<MiruTenantId> tenantIds = getAllTenantIds();
        for (MiruTenantId tenantId : tenantIds) {
            List<MiruPartitionStatus> status = getAllPartitionStatus(tenantId);
            int[] count = { 0 };
            filterCleanup(status, false, (partitionStatus, isLatest) -> {
                removePartition(tenantId, partitionStatus.getPartitionId());
                count[0]++;
                return true;
            });
            if (count[0] > 0) {
                LOG.info("Removed {} partitions for tenant {}", count, tenantId);
            }
        }
        LOG.info("Finished scan for partitions to clean up");
    }

    public void filterCleanup(List<MiruPartitionStatus> status, boolean includeLatestPartitions, PartitionStatusStream stream) throws Exception {
        MiruPartitionId latestPartitionId = null;
        for (MiruPartitionStatus partitionStatus : status) {
            if (latestPartitionId == null || latestPartitionId.compareTo(partitionStatus.getPartitionId()) < 0) {
                latestPartitionId = partitionStatus.getPartitionId();
            }
        }

        for (MiruPartitionStatus partitionStatus : status) {
            boolean isLatest = partitionStatus.getPartitionId().equals(latestPartitionId);
            if (partitionStatus.getCleanupAfterTimestamp() > 0
                && System.currentTimeMillis() > partitionStatus.getCleanupAfterTimestamp()
                && (includeLatestPartitions || !isLatest)) {
                if (!stream.stream(partitionStatus, isLatest)) {
                    break;
                }
            }
        }
    }

    public interface PartitionStatusStream {

        boolean stream(MiruPartitionStatus partitionStatus, boolean isLatest) throws Exception;
    }

    public List<MiruPartitionStatus> getAllPartitionStatus(MiruTenantId tenantId) throws Exception {
        MiruPartitionId largestPartitionId = getLargestPartitionId(tenantId);
        return clusterClient.getPartitionStatus(tenantId, largestPartitionId);
    }

    public void removePartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        activityWALWriter.removePartition(tenantId, partitionId);
        // We do not remove ingress because it acts as tombstones
    }

    private long packTimestamp(long millisIntoTheFuture) {
        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        long epochTime = System.currentTimeMillis() + millisIntoTheFuture - JiveEpochTimestampProvider.JIVE_EPOCH;
        return snowflakeIdPacker.pack(epochTime, 0, 0);
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

    public HostPort[] getTenantPartitionRoutingGroup(RoutingGroupType routingGroupType,
        MiruTenantId tenantId,
        MiruPartitionId partitionId,
        boolean createIfAbsent) throws Exception {
        if (routingGroupType == RoutingGroupType.activity) {
            return activityWALReader.getRoutingGroup(tenantId, partitionId, createIfAbsent);
        } else {
            throw new IllegalArgumentException("Type does not have tenant-partition routing: " + routingGroupType.name());
        }
    }

    public HostPort[] getTenantRoutingGroup(RoutingGroupType routingGroupType,
        MiruTenantId tenantId,
        boolean createIfAbsent) throws Exception {
        if (routingGroupType == RoutingGroupType.readTracking) {
            return readTrackingWALReader.getRoutingGroup(tenantId, createIfAbsent);
        } else {
            throw new IllegalArgumentException("Type does not have tenant-stream routing: " + routingGroupType.name());
        }
    }

    public List<MiruTenantId> getAllTenantIds() throws Exception {
        return walLookup.allTenantIds(repairLookupCallback);
    }

    public void writeActivity(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        boolean onlyBoundaries = true;

        // gather before we write because the writer may clear the list
        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            if (!partitionedActivity.type.isBoundaryType()) {
                onlyBoundaries = false;
                break;
            }
        }

        RangeMinMax partitionMinMax = activityWALWriter.write(tenantId, partitionId, partitionedActivities);
        walLookup.add(tenantId, partitionId);

        if (!onlyBoundaries) {
            clusterClient.updateIngress(new MiruIngressUpdate(tenantId, partitionId, partitionMinMax, System.currentTimeMillis(), false));
        }
    }

    public void writeReadTracking(MiruTenantId tenantId, MiruStreamId streamId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        readTrackingWALWriter.write(tenantId, streamId, partitionedActivities);
    }

    public MiruPartitionId getLargestPartitionId(MiruTenantId tenantId) throws Exception {
        return walLookup.largestPartitionId(tenantId, repairLookupCallback);
    }

    public WriterCursor getCursorForWriterId(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) throws Exception {
        return activityWALReader.getCursorForWriterId(tenantId, partitionId, writerId);
    }

    public MiruActivityWALStatus getActivityWALStatusForTenant(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return activityWALReader.getStatus(tenantId, partitionId);
    }

    public long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return activityWALReader.oldestActivityClockTimestamp(tenantId, partitionId);
    }

    public List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, MiruPartitionId partitionId, Long[] timestamps) throws Exception {
        return activityWALReader.getVersionedEntries(tenantId, partitionId, timestamps);
    }

    public StreamBatch<MiruWALEntry, AmzaCursor> getActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        AmzaCursor cursor,
        int batchSize,
        long stopAtTimestamp,
        MutableLong bytesCount)
        throws Exception {

        List<MiruWALEntry> activities = new ArrayList<>();
        AmzaCursor nextCursor = activityWALReader.stream(tenantId, partitionId, cursor, batchSize, stopAtTimestamp,
            (collisionId, partitionedActivity, timestamp) -> {
                activities.add(new MiruWALEntry(collisionId, timestamp, partitionedActivity));
                return activities.size() < batchSize;
            });

        return new StreamBatch<>(activities, nextCursor, false, null);
    }

    public StreamBatch<MiruWALEntry, AmzaSipCursor> sipActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        AmzaSipCursor cursor,
        Set<TimeAndVersion> lastSeen,
        final int batchSize) throws Exception {

        List<MiruWALEntry> activities = new ArrayList<>();
        Set<TimeAndVersion> suppressed = Sets.newHashSet();
        boolean[] endOfWAL = { false };
        AmzaSipCursor nextCursor = activityWALReader.streamSip(tenantId, partitionId, cursor, lastSeen, batchSize,
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

    public StreamBatch<MiruWALEntry, AmzaSipCursor> getRead(MiruTenantId tenantId,
        MiruStreamId streamId,
        AmzaSipCursor sipCursor,
        long oldestTimestamp,
        int batchSize,
        boolean createIfAbsent) throws Exception {

        long[] minEventId = { -1L };
        int[] count = new int[1];
        AmzaSipCursor nextCursor = readTrackingWALReader.streamSip(tenantId, streamId, sipCursor, batchSize, (eventId, timestamp) -> {
            minEventId[0] = (minEventId[0] == -1) ? eventId : Math.min(minEventId[0], eventId);
            count[0]++;
            return count[0] < batchSize;
        });

        // Take either the oldest eventId or the oldest readtracking sip time
        minEventId[0] = Math.min(minEventId[0], oldestTimestamp);
        AmzaCursor cursor = minEventId[0] > 0 ? readTrackingWALReader.getCursor(minEventId[0]) : null;

        List<MiruWALEntry> batch = new ArrayList<>();
        if (cursor != null) {
            readTrackingWALReader.stream(tenantId, streamId, cursor, batchSize, (collisionId, partitionedActivity, timestamp) -> {
                batch.add(new MiruWALEntry(collisionId, timestamp, partitionedActivity));
                return true; // always consume completely
            });
        }
        return new StreamBatch<>(batch, nextCursor, batch.size() < batchSize, null);
    }

}
