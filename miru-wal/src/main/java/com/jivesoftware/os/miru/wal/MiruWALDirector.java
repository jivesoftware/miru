package com.jivesoftware.os.miru.wal;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruReadSipEntry;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.lookup.MiruWALLookup;
import com.jivesoftware.os.miru.wal.lookup.RangeMinMax;
import com.jivesoftware.os.miru.wal.partition.MiruPartitionIdProvider;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
    private final MiruPartitionIdProvider partitionIdProvider;
    private final MiruReadTrackingWALReader readTrackingWALReader;

    private final MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();

    public MiruWALDirector(MiruWALLookup walLookup,
        MiruActivityWALReader<C, S> activityWALReader,
        MiruActivityWALWriter activityWALWriter,
        MiruPartitionIdProvider partitionIdProvider,
        MiruReadTrackingWALReader readTrackingWALReader) {
        this.walLookup = walLookup;
        this.activityWALReader = activityWALReader;
        this.activityWALWriter = activityWALWriter;
        this.partitionIdProvider = partitionIdProvider;
        this.readTrackingWALReader = readTrackingWALReader;
    }

    public void repairBoundaries() throws Exception {
        List<MiruTenantId> tenantIds = walLookup.allTenantIds();
        for (MiruTenantId tenantId : tenantIds) {
            MiruPartitionId latestPartitionId = partitionIdProvider.getLargestPartitionIdAcrossAllWriters(tenantId);
            if (latestPartitionId != null) {
                for (MiruPartitionId partitionId = latestPartitionId.prev(); partitionId != null; partitionId = partitionId.prev()) {
                    MiruActivityWALStatus status = activityWALReader.getStatus(tenantId, partitionId);
                    if (!status.begins.equals(status.ends)) {
                        for (int begin : status.begins) {
                            if (!status.ends.contains(begin)) {
                                activityWALWriter.write(tenantId, Arrays.asList(partitionedActivityFactory.end(begin, partitionId, tenantId, -1)));
                                LOG.info("Added missing 'end' to WAL for {} {}", tenantId, partitionId);
                            }
                        }
                        for (int end : status.ends) {
                            if (!status.begins.contains(end)) {
                                activityWALWriter.write(tenantId, Arrays.asList(partitionedActivityFactory.begin(end, partitionId, tenantId, -1)));
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
            walLookup.streamRanges(tenantId, null, (partitionId, type, timestamp) -> {
                found.add(partitionId);
                if (type == MiruWALLookup.RangeType.orderIdMax && timestamp == Long.MAX_VALUE) {
                    broken.add(partitionId);
                }
                return true;
            });

            int count = 0;
            MiruPartitionId latestPartitionId = partitionIdProvider.getLargestPartitionIdAcrossAllWriters(tenantId);
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
                    walLookup.putRange(tenantId, checkPartitionId, minMax);
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
        activityWALReader.delete(tenantId, partitionId, badKeys);
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
        activityWALReader.deleteSip(tenantId, partitionId, badKeys);
    }

    @Override
    public List<MiruTenantId> getAllTenantIds() throws Exception {
        return walLookup.allTenantIds();
    }

    @Override
    public MiruPartitionId getLargestPartitionIdAcrossAllWriters(MiruTenantId tenantId) throws Exception {
        return partitionIdProvider.getLargestPartitionIdAcrossAllWriters(tenantId);
    }

    @Override
    public List<MiruActivityWALStatus> getPartitionStatus(MiruTenantId tenantId, List<MiruPartitionId> partitionIds) throws Exception {
        List<MiruActivityWALStatus> statuses = new ArrayList<>();
        for (MiruPartitionId partitionId : partitionIds) {
            statuses.add(activityWALReader.getStatus(tenantId, partitionId));
        }
        return statuses;
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
    public MiruLookupRange lookupRange(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        final MiruLookupRange lookupRange = new MiruLookupRange(partitionId.getId(), -1, -1, -1, -1); // ugly
        walLookup.streamRanges(tenantId, partitionId, (streamPartitionId, type, timestamp) -> {
            if (partitionId.equals(streamPartitionId)) {
                if (type == MiruWALLookup.RangeType.clockMin) {
                    lookupRange.minClock = timestamp;
                } else if (type == MiruWALLookup.RangeType.clockMax) {
                    lookupRange.maxClock = timestamp;
                } else if (type == MiruWALLookup.RangeType.orderIdMin) {
                    lookupRange.minOrderId = timestamp;
                } else if (type == MiruWALLookup.RangeType.orderIdMax) {
                    lookupRange.maxOrderId = timestamp;
                }
                return true;
            } else {
                return false;
            }
        });
        return lookupRange;
    }

    @Override
    public Collection<MiruLookupRange> lookupRanges(MiruTenantId tenantId) throws Exception {
        final Map<MiruPartitionId, MiruLookupRange> partitionLookupRange = Maps.newHashMap();
        walLookup.streamRanges(tenantId, null, (partitionId, type, timestamp) -> {
            MiruLookupRange lookupRange = partitionLookupRange.get(partitionId);
            if (lookupRange == null) {
                lookupRange = new MiruLookupRange(partitionId.getId(), -1, -1, -1, -1); // ugly
                partitionLookupRange.put(partitionId, lookupRange);
            }
            if (type == MiruWALLookup.RangeType.clockMin) {
                lookupRange.minClock = timestamp;
            } else if (type == MiruWALLookup.RangeType.clockMax) {
                lookupRange.maxClock = timestamp;
            } else if (type == MiruWALLookup.RangeType.orderIdMin) {
                lookupRange.minOrderId = timestamp;
            } else if (type == MiruWALLookup.RangeType.orderIdMax) {
                lookupRange.maxOrderId = timestamp;
            }
            return true;
        });
        return partitionLookupRange.values();
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
