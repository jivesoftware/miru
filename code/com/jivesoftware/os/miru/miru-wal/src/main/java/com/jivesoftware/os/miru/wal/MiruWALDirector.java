package com.jivesoftware.os.miru.wal;

import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruReadSipEntry;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALEntry;
import com.jivesoftware.os.miru.api.wal.Sip;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.lookup.MiruActivityLookupTable;
import com.jivesoftware.os.miru.wal.partition.MiruPartitionIdProvider;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.mutable.MutableByte;
import org.apache.commons.lang.mutable.MutableLong;

/**
 *
 */
public class MiruWALDirector implements MiruWALClient {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruActivityLookupTable activityLookupTable;
    private final MiruActivityWALReader activityWALReader;
    private final MiruActivityWALWriter activityWALWriter;
    private final MiruPartitionIdProvider partitionIdProvider;
    private final MiruReadTrackingWALReader readTrackingWALReader;

    private final MiruPartitionedActivityFactory partitionedActivityFactory = new MiruPartitionedActivityFactory();

    public MiruWALDirector(MiruActivityLookupTable activityLookupTable,
        MiruActivityWALReader activityWALReader,
        MiruActivityWALWriter activityWALWriter,
        MiruPartitionIdProvider partitionIdProvider,
        MiruReadTrackingWALReader readTrackingWALReader) {
        this.activityLookupTable = activityLookupTable;
        this.activityWALReader = activityWALReader;
        this.activityWALWriter = activityWALWriter;
        this.partitionIdProvider = partitionIdProvider;
        this.readTrackingWALReader = readTrackingWALReader;
    }

    public void repairActivityWAL() throws Exception {
        List<MiruTenantId> tenantIds = activityLookupTable.allTenantIds();
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

    private long packTimestamp(long millisIntoTheFuture) {
        SnowflakeIdPacker snowflakeIdPacker = new SnowflakeIdPacker();
        long epochTime = System.currentTimeMillis() + millisIntoTheFuture - JiveEpochTimestampProvider.JIVE_EPOCH;
        return snowflakeIdPacker.pack(epochTime, 0, 0);
    }

    public void sanitizeActivityWAL(final MiruTenantId tenantId, final MiruPartitionId partitionId) throws Exception {
        long afterTimestamp = packTimestamp(TimeUnit.DAYS.toMillis(1));
        final List<MiruActivityWALColumnKey> badKeys = Lists.newArrayList();
        activityWALReader.stream(tenantId, partitionId, MiruPartitionedActivity.Type.ACTIVITY.getSort(),
            afterTimestamp, 10_000, new MiruActivityWALReader.StreamMiruActivityWAL() {
                @Override
                public boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                    if (partitionedActivity.type.isActivityType()) {
                        LOG.warn("Sanitizer is removing activity " + collisionId + "/" + partitionedActivity.timestamp + " from "
                            + tenantId + "/" + partitionId);
                        badKeys.add(new MiruActivityWALColumnKey(partitionedActivity.type.getSort(), collisionId));
                    }
                    return partitionedActivity.type.isActivityType();
                }
            });
        activityWALReader.delete(tenantId, partitionId, badKeys);
    }

    public void sanitizeActivitySipWAL(final MiruTenantId tenantId, final MiruPartitionId partitionId) throws Exception {
        final long afterTimestamp = packTimestamp(TimeUnit.DAYS.toMillis(1));
        final List<MiruActivitySipWALColumnKey> badKeys = Lists.newArrayList();
        activityWALReader.streamSip(tenantId, partitionId, MiruPartitionedActivity.Type.ACTIVITY.getSort(),
            new Sip(0, 0), 10_000,
            new MiruActivityWALReader.StreamMiruActivityWAL() {
                @Override
                public boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                    if (partitionedActivity.timestamp > afterTimestamp && partitionedActivity.type.isActivityType()) {
                        LOG.warn("Sanitizer is removing sip activity " + collisionId + "/" + partitionedActivity.timestamp + " from "
                            + tenantId + "/" + partitionId);
                        badKeys.add(new MiruActivitySipWALColumnKey(partitionedActivity.type.getSort(), collisionId, timestamp));
                    }
                    return partitionedActivity.type.isActivityType();
                }
            });
        activityWALReader.deleteSip(tenantId, partitionId, badKeys);
    }

    @Override
    public List<MiruTenantId> getAllTenantIds() throws Exception {
        return activityLookupTable.allTenantIds();
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
        activityLookupTable.stream(tenantId, afterTimestamp, new MiruActivityLookupTable.StreamLookupEntry() {

            @Override
            public boolean stream(long activityTimestamp, MiruActivityLookupEntry entry, long version) throws Exception {
                batch.add(new MiruLookupEntry(activityTimestamp, version, entry));
                return batch.size() < batchSize;
            }
        });
        return batch;
    }

    @Override
    public StreamBatch<MiruWALEntry, SipActivityCursor> sipActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        SipActivityCursor cursor,
        final int batchSize) throws Exception {

        final List<MiruWALEntry> batch = new ArrayList<>();
        final MutableLong lastCollisionId = new MutableLong(cursor.collisionId);
        final MutableLong lastSipId = new MutableLong(cursor.sipId);
        final MutableByte sortOrder = new MutableByte(cursor.sort);

        activityWALReader.streamSip(tenantId, partitionId, cursor.sort, new Sip(cursor.collisionId, cursor.sipId), batchSize,
            new MiruActivityWALReader.StreamMiruActivityWAL() {

                @Override
                public boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                    batch.add(new MiruWALEntry(collisionId, timestamp, partitionedActivity));
                    lastCollisionId.setValue(collisionId);
                    lastSipId.setValue(partitionedActivity.timestamp);
                    sortOrder.setValue(partitionedActivity.type.getSort());
                    return batch.size() < batchSize;
                }
            });

        SipActivityCursor nextCursor;
        if (batch.size() < batchSize) {
            nextCursor = null;
        } else if (lastSipId.longValue() == Long.MAX_VALUE) {
            if (lastCollisionId.longValue() == Long.MAX_VALUE) {
                if (sortOrder.byteValue() == Byte.MAX_VALUE) {
                    nextCursor = null;
                } else {
                    nextCursor = new SipActivityCursor((byte) (sortOrder.byteValue() + 1), Long.MIN_VALUE, Long.MIN_VALUE);
                }
            } else {
                nextCursor = new SipActivityCursor(sortOrder.byteValue(), lastCollisionId.longValue() + 1, Long.MIN_VALUE);
            }
        } else {
            nextCursor = new SipActivityCursor(sortOrder.byteValue(), lastCollisionId.longValue(), lastSipId.longValue() + 1);
        }
        return new StreamBatch<>(batch, nextCursor);
    }

    @Override
    public StreamBatch<MiruWALEntry, GetActivityCursor> getActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        GetActivityCursor cursor,
        final int batchSize)
        throws Exception {

        final List<MiruWALEntry> batch = new ArrayList<>();
        final MutableLong lastCollisionId = new MutableLong(cursor.collisionId);
        final MutableByte sortOrder = new MutableByte(cursor.sort);
        activityWALReader.stream(tenantId, partitionId, cursor.sort, cursor.collisionId, batchSize,
            new MiruActivityWALReader.StreamMiruActivityWAL() {

                @Override
                public boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                    batch.add(new MiruWALEntry(collisionId, timestamp, partitionedActivity));
                    lastCollisionId.setValue(collisionId);
                    sortOrder.setValue(partitionedActivity.type.getSort());
                    return batch.size() < batchSize;
                }
            });

        GetActivityCursor nextCursor;
        if (batch.size() < batchSize) {
            nextCursor = null;
        } else if (lastCollisionId.longValue() == Long.MAX_VALUE) {
            if (sortOrder.byteValue() == Byte.MAX_VALUE) {
                nextCursor = null;
            } else {
                nextCursor = new GetActivityCursor((byte) (sortOrder.byteValue() + 1), Long.MIN_VALUE);
            }
        } else {
            nextCursor = new GetActivityCursor(sortOrder.byteValue(), lastCollisionId.longValue() + 1);
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

        readTrackingWALReader.streamSip(tenantId, streamId, cursor.eventId, new MiruReadTrackingWALReader.StreamReadTrackingSipWAL() {

            @Override
            public boolean stream(long eventId, long timestamp) throws Exception {
                batch.add(new MiruReadSipEntry(eventId, timestamp));
                lastEventId.setValue(eventId);
                lastSipId.setValue(timestamp);
                return batch.size() < batchSize;
            }
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

    @Override
    public StreamBatch<MiruWALEntry, GetReadCursor> getRead(MiruTenantId tenantId,
        MiruStreamId streamId,
        GetReadCursor cursor,
        final int batchSize) throws Exception {

        final List<MiruWALEntry> batch = new ArrayList<>();
        final MutableLong lastEventId = new MutableLong(cursor.eventId);

        readTrackingWALReader.stream(tenantId, streamId, cursor.eventId, new MiruReadTrackingWALReader.StreamReadTrackingWAL() {

            @Override
            public boolean stream(long eventId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                batch.add(new MiruWALEntry(eventId, timestamp, partitionedActivity));
                lastEventId.setValue(eventId);
                return batch.size() < batchSize;
            }
        });

        GetReadCursor nextCursor;
        if (batch.size() < batchSize || lastEventId.longValue() == Long.MAX_VALUE) {
            nextCursor = null;
        } else {
            nextCursor = new GetReadCursor(lastEventId.longValue() + 1);
        }
        return new StreamBatch<>(batch, nextCursor);
    }

}
