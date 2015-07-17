package com.jivesoftware.os.miru.wal;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruIngressUpdate;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruReadSipEntry;
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
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 */
public class MiruWALDirector<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> implements MiruWALClient<C, S> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruWALLookup walLookup;
    private final MiruActivityWALReader<C, S> activityWALReader;
    private final MiruActivityWALWriter activityWALWriter;
    private final MiruReadTrackingWALReader readTrackingWALReader;
    private final MiruReadTrackingWALWriter readTrackingWALWriter;
    private final MiruClusterClient clusterClient;

    private final MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();

    public MiruWALDirector(MiruWALLookup walLookup,
        MiruActivityWALReader<C, S> activityWALReader,
        MiruActivityWALWriter activityWALWriter,
        MiruReadTrackingWALReader readTrackingWALReader,
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
        List<MiruTenantId> tenantIds = walLookup.allTenantIds();
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

    public void repairRanges() throws Exception {
        List<MiruTenantId> tenantIds = walLookup.allTenantIds();
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
                    final RangeMinMax minMax = new RangeMinMax();
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

            LOG.info("Repaired ranges in {} partitions for tenant {}", count, tenantId);
        }
    }

    public void removePartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        activityWALWriter.removePartition(tenantId, partitionId);
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
        activityWALReader.streamSip(tenantId, partitionId, null, 10_000,
            (collisionId, partitionedActivity, timestamp) -> {
                if (partitionedActivity.timestamp > afterTimestamp && partitionedActivity.type.isActivityType()) {
                    LOG.warn("Sanitizer is removing sip activity " + collisionId + "/" + partitionedActivity.timestamp + " from "
                        + tenantId + "/" + partitionId);
                    badKeys.add(new MiruActivitySipWALColumnKey(partitionedActivity.type.getSort(), collisionId, timestamp));
                }
                return partitionedActivity.type.isActivityType();
            });
        activityWALWriter.deleteSip(tenantId, partitionId, badKeys);
    }

    @Override
    public HostPort[] getTenantRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId) throws Exception {
        if (routingGroupType == RoutingGroupType.lookup) {
            return walLookup.getRoutingGroup(tenantId);
        } else {
            throw new IllegalArgumentException("Type is not have tenant routing: " + routingGroupType.name());
        }
    }

    @Override
    public HostPort[] getTenantPartitionRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        if (routingGroupType == RoutingGroupType.activity) {
            return activityWALReader.getRoutingGroup(tenantId, partitionId);
        } else {
            throw new IllegalArgumentException("Type is not have tenant-partition routing: " + routingGroupType.name());
        }
    }

    @Override
    public HostPort[] getTenantStreamRoutingGroup(RoutingGroupType routingGroupType, MiruTenantId tenantId, MiruStreamId streamId) throws Exception {
        if (routingGroupType == RoutingGroupType.readTracking) {
            return readTrackingWALReader.getRoutingGroup(tenantId, streamId);
        } else {
            throw new IllegalArgumentException("Type is not have tenant-stream routing: " + routingGroupType.name());
        }
    }

    @Override
    public List<MiruTenantId> getAllTenantIds() throws Exception {
        return walLookup.allTenantIds();
    }

    @Override
    public void writeActivity(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        RangeMinMax partitionMinMax = activityWALWriter.write(tenantId, partitionId, partitionedActivities);
        clusterClient.updateIngress(new MiruIngressUpdate(tenantId, partitionId, partitionMinMax, System.currentTimeMillis(), false));
    }

    @Override
    public void writeLookup(MiruTenantId tenantId, List<MiruVersionedActivityLookupEntry> entries) throws Exception {
        walLookup.add(tenantId, entries);
    }

    @Override
    public void writeReadTracking(MiruTenantId tenantId, MiruStreamId streamId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        readTrackingWALWriter.write(tenantId, streamId, partitionedActivities);
    }

    @Override
    public MiruPartitionId getLargestPartitionId(MiruTenantId tenantId) throws Exception {
        return activityWALReader.largestPartitionId(tenantId);
    }

    @Override
    public WriterCursor getCursorForWriterId(MiruTenantId tenantId, int writerId) throws Exception {
        return activityWALReader.getCursorForWriterId(tenantId, writerId);
    }

    @Override
    public MiruActivityWALStatus getPartitionStatus(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return activityWALReader.getStatus(tenantId, partitionId);
    }

    @Override
    public long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return activityWALReader.oldestActivityClockTimestamp(tenantId, partitionId);
    }

    @Override
    public List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, Long[] timestamps) throws Exception {
        return walLookup.getVersionedEntries(tenantId, timestamps);
    }

    @Override
    public List<MiruLookupEntry> lookupActivity(MiruTenantId tenantId, long afterTimestamp, final int batchSize) throws Exception {
        final List<MiruLookupEntry> batch = new ArrayList<>();
        walLookup.stream(tenantId, afterTimestamp, (activityTimestamp, entry, version) -> {
            batch.add(new MiruLookupEntry(activityTimestamp, version, entry));
            return batch.size() < batchSize;
        });
        return batch;
    }

    @Override
    public StreamBatch<MiruWALEntry, C> getActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        C cursor,
        final int batchSize)
        throws Exception {

        final List<MiruWALEntry> batch = new ArrayList<>();
        C nextCursor = activityWALReader.stream(tenantId, partitionId, cursor, batchSize,
            (collisionId, partitionedActivity, timestamp) -> {
                batch.add(new MiruWALEntry(collisionId, timestamp, partitionedActivity));
                return batch.size() < batchSize;
            });

        return new StreamBatch<>(batch, nextCursor);
    }

    @Override
    public StreamBatch<MiruWALEntry, S> sipActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        S cursor,
        final int batchSize) throws Exception {

        final List<MiruWALEntry> batch = new ArrayList<>();
        S nextCursor = activityWALReader.streamSip(tenantId, partitionId, cursor, batchSize,
            (long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) -> {
                batch.add(new MiruWALEntry(collisionId, timestamp, partitionedActivity));
                return batch.size() < batchSize;
            });

        return new StreamBatch<>(batch, nextCursor);
    }

    @Override
    public StreamBatch<MiruWALEntry, GetReadCursor> getRead(MiruTenantId tenantId,
        MiruStreamId streamId,
        GetReadCursor cursor,
        final int batchSize) throws Exception {

        final List<MiruWALEntry> batch = new ArrayList<>();
        final MutableLong lastEventId = new MutableLong(cursor.eventId);

        readTrackingWALReader.stream(tenantId, streamId, cursor.eventId, (eventId, partitionedActivity, timestamp) -> {
            batch.add(new MiruWALEntry(eventId, timestamp, partitionedActivity));
            lastEventId.setValue(eventId);
            return batch.size() < batchSize;
        });

        GetReadCursor nextCursor;
        if (batch.size() < batchSize || lastEventId.longValue() == Long.MAX_VALUE) {
            nextCursor = null;
        } else {
            nextCursor = new GetReadCursor(lastEventId.longValue() + 1);
        }
        return new StreamBatch<>(batch, nextCursor);
    }

    @Override
    public StreamBatch<MiruReadSipEntry, SipReadCursor> sipRead(MiruTenantId tenantId, MiruStreamId streamId, SipReadCursor cursor,
        final int batchSize)
        throws Exception {

        final List<MiruReadSipEntry> batch = new ArrayList<>();
        final MutableLong lastEventId = new MutableLong(cursor.eventId);
        final MutableLong lastSipId = new MutableLong(cursor.sipId);

        readTrackingWALReader.streamSip(tenantId, streamId, cursor.eventId, (eventId, timestamp) -> {
            batch.add(new MiruReadSipEntry(eventId, timestamp));
            lastEventId.setValue(eventId);
            lastSipId.setValue(timestamp);
            return batch.size() < batchSize;
        });

        SipReadCursor nextCursor;
        if (batch.size() < batchSize) {
            nextCursor = null;
        } else if (lastSipId.longValue() == Long.MAX_VALUE) {
            if (lastEventId.longValue() == Long.MAX_VALUE) {
                nextCursor = null;
            } else {
                nextCursor = new SipReadCursor(lastEventId.longValue() + 1, Long.MIN_VALUE);
            }
        } else {
            nextCursor = new SipReadCursor(lastEventId.longValue(), lastSipId.longValue() + 1);
        }
        return new StreamBatch<>(batch, nextCursor);
    }

}
