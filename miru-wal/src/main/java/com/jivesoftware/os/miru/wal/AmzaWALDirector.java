package com.jivesoftware.os.miru.wal;

import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.StreamIdPartitionedActivities;
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
import com.jivesoftware.os.miru.api.wal.MiruWALClient.OldestReadResult;
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
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.api.HealthTimer;
import com.jivesoftware.os.routing.bird.health.api.TimerHealthCheckConfig;
import com.jivesoftware.os.routing.bird.health.checkers.TimerHealthChecker;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.commons.lang.mutable.MutableLong;
import org.merlin.config.defaults.DoubleDefault;
import org.merlin.config.defaults.StringDefault;

/**
 *
 */
public class AmzaWALDirector {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public interface GetActivityLatency extends TimerHealthCheckConfig {
        @StringDefault("getActivity>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to getActivity.")
        @Override
        String getDescription();

        @DoubleDefault(30000d) /// 30sec
        @Override
        Double get95ThPercentileMax();
    }

    private static final HealthTimer getActivityLatency = HealthFactory.getHealthTimer(GetActivityLatency.class, TimerHealthChecker.FACTORY);

    public interface SipActivityLatency extends TimerHealthCheckConfig {
        @StringDefault("sipActivity>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to sipActivity.")
        @Override
        String getDescription();

        @DoubleDefault(30000d) /// 30sec
        @Override
        Double get95ThPercentileMax();
    }

    private static final HealthTimer sipActivityLatency = HealthFactory.getHealthTimer(SipActivityLatency.class, TimerHealthChecker.FACTORY);

    public interface OldestReadEventIdLatency extends TimerHealthCheckConfig {
        @StringDefault("oldestReadEventId>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to oldestReadEventId.")
        @Override
        String getDescription();

        @DoubleDefault(30000d) /// 30sec
        @Override
        Double get95ThPercentileMax();
    }

    private static final HealthTimer oldestReadEventIdLatency = HealthFactory.getHealthTimer(OldestReadEventIdLatency.class, TimerHealthChecker.FACTORY);

    public interface ScanReadLatency extends TimerHealthCheckConfig {
        @StringDefault("scanRead>latency")
        @Override
        String getName();

        @StringDefault("How long its taking to scanRead.")
        @Override
        String getDescription();

        @DoubleDefault(30000d) /// 30sec
        @Override
        Double get95ThPercentileMax();
    }

    private static final HealthTimer scanReadLatency = HealthFactory.getHealthTimer(ScanReadLatency.class, TimerHealthChecker.FACTORY);

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

    public void writeReadTracking(MiruTenantId tenantId, List<StreamIdPartitionedActivities> streamActivities) throws Exception {
        readTrackingWALWriter.write(tenantId, streamActivities);
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

    public long getActivityCount(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return activityWALReader.count(tenantId, partitionId);
    }

    public StreamBatch<MiruWALEntry, AmzaCursor> getActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        AmzaCursor cursor,
        int batchSize,
        long stopAtTimestamp,
        MutableLong bytesCount)
        throws Exception {

        getActivityLatency.startTimer();
        try {
            List<MiruWALEntry> activities = new ArrayList<>();
            AmzaCursor nextCursor = activityWALReader.stream(tenantId, partitionId, cursor, batchSize, stopAtTimestamp,
                (collisionId, partitionedActivity, timestamp) -> {
                    activities.add(new MiruWALEntry(collisionId, timestamp, partitionedActivity));
                    return activities.size() < batchSize;
                });

            return new StreamBatch<>(activities, nextCursor, false, null);
        } finally {
            getActivityLatency.stopTimer("Get activity latency", "Check partition health");
        }
    }

    public StreamBatch<MiruWALEntry, AmzaSipCursor> sipActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        AmzaSipCursor cursor,
        Set<TimeAndVersion> lastSeen,
        final int batchSize) throws Exception {

        sipActivityLatency.startTimer();
        try {
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
        } finally {
            sipActivityLatency.stopTimer("Sip activity latency", "Check partition health");
        }
    }

    public OldestReadResult<AmzaSipCursor> oldestReadEventId(MiruTenantId tenantId,
        MiruStreamId streamId,
        AmzaSipCursor sipCursor) throws Exception {

        oldestReadEventIdLatency.startTimer();
        try {
            long[] minEventId = { -1L };
            int[] count = { 0 };
            AmzaSipCursor nextCursor = readTrackingWALReader.streamSip(tenantId, streamId, sipCursor, Integer.MAX_VALUE, (eventId, timestamp) -> {
                minEventId[0] = (minEventId[0] == -1) ? eventId : Math.min(minEventId[0], eventId);
                count[0]++;
                return true; // always consume completely
            });
            LOG.inc("oldestReadEventId>count>pow>" + FilerIO.chunkPower(count[0], 0));
            return new OldestReadResult<>(minEventId[0], nextCursor, nextCursor.endOfStream);
        } finally {
            oldestReadEventIdLatency.stopTimer("Oldest read eventId latency", "Check partition health");
        }
    }

    public StreamBatch<MiruWALEntry, Long> scanRead(MiruTenantId tenantId,
        MiruStreamId streamId,
        long oldestTimestamp,
        int batchSize) throws Exception {

        scanReadLatency.startTimer();
        try {
            List<MiruWALEntry> batch = new ArrayList<>();
            long nextCursor = readTrackingWALReader.stream(tenantId, streamId, oldestTimestamp, (collisionId, partitionedActivity, timestamp) -> {
                batch.add(new MiruWALEntry(collisionId, timestamp, partitionedActivity));
                return (batch.size() < batchSize);
            });
            LOG.inc("scanRead>batch>pow>" + FilerIO.chunkPower(batch.size(), 0));
            return new StreamBatch<>(batch, nextCursor, batch.size() < batchSize, null);
        } finally {
            scanReadLatency.stopTimer("Scan read latency", "Check partition health");
        }
    }

}
