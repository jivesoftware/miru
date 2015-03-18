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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
        final AtomicLong largestCollisionId = new AtomicLong(cursor.collisionId);
        final AtomicLong largestSipId = new AtomicLong(cursor.sipId);
        final AtomicReference<Byte> sortOrder = new AtomicReference<>(cursor.sort);

        activityWALReader.streamSip(tenantId, partitionId, cursor.sort, new Sip(cursor.collisionId, cursor.sipId), batchSize,
            new MiruActivityWALReader.StreamMiruActivityWAL() {

                @Override
                public boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                    batch.add(new MiruWALEntry(collisionId, timestamp, partitionedActivity));
                    if (collisionId > largestCollisionId.get()) {
                        largestCollisionId.set(collisionId);
                        largestSipId.set(partitionedActivity.timestamp);
                        sortOrder.set(partitionedActivity.type.getSort());
                    }
                    return batch.size() < batchSize;
                }
            });

        return new StreamBatch<>(batch, new SipActivityCursor(sortOrder.get(), largestCollisionId.get(), largestSipId.get() + 1));
    }

    @Override
    public StreamBatch<MiruWALEntry, GetActivityCursor> getActivity(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        GetActivityCursor cursor,
        final int batchSize)
        throws Exception {

        final List<MiruWALEntry> batch = new ArrayList<>();
        final AtomicLong largestCollisionId = new AtomicLong(cursor.collisionId);
        final AtomicReference<Byte> sortOrder = new AtomicReference<>(cursor.sort);
        activityWALReader.stream(tenantId, partitionId, cursor.sort, cursor.collisionId, batchSize,
            new MiruActivityWALReader.StreamMiruActivityWAL() {

                @Override
                public boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                    batch.add(new MiruWALEntry(collisionId, timestamp, partitionedActivity));
                    if (collisionId > largestCollisionId.get()) {
                        largestCollisionId.set(collisionId);
                        sortOrder.set(partitionedActivity.type.getSort());
                    }
                    return batch.size() < batchSize;
                }
            });

        return new StreamBatch<>(batch, new GetActivityCursor(sortOrder.get(), largestCollisionId.get() + 1));
    }

    @Override
    public StreamBatch<MiruReadSipEntry, SipReadCursor> sipRead(MiruTenantId tenantId, MiruStreamId streamId, SipReadCursor cursor,
        final int batchSize)
        throws Exception {

        final List<MiruReadSipEntry> batch = new ArrayList<>();
        final AtomicLong largestEventId = new AtomicLong(cursor.eventId);
        final AtomicLong largestSipId = new AtomicLong(cursor.sipId);

        readTrackingWALReader.streamSip(tenantId, streamId, cursor.eventId, new MiruReadTrackingWALReader.StreamReadTrackingSipWAL() {

            @Override
            public boolean stream(long eventId, long timestamp) throws Exception {
                batch.add(new MiruReadSipEntry(eventId, timestamp));
                if (eventId > largestEventId.get()) {
                    largestEventId.set(eventId);
                    largestSipId.set(timestamp);
                }
                return batch.size() < batchSize;
            }
        });

        return new StreamBatch<>(batch, new SipReadCursor(largestEventId.get(), largestSipId.get() + 1));
    }

    @Override
    public StreamBatch<MiruWALEntry, GetReadCursor> getRead(MiruTenantId tenantId,
        MiruStreamId streamId,
        GetReadCursor cursor,
        final int batchSize) throws Exception {

        final List<MiruWALEntry> batch = new ArrayList<>();
        final AtomicLong largestEventId = new AtomicLong(cursor.eventId);

        readTrackingWALReader.stream(tenantId, streamId, cursor.eventId, new MiruReadTrackingWALReader.StreamReadTrackingWAL() {

            @Override
            public boolean stream(long eventId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception {
                batch.add(new MiruWALEntry(eventId, timestamp, partitionedActivity));
                if (eventId > largestEventId.get()) {
                    largestEventId.set(eventId);
                }
                return batch.size() < batchSize;
            }
        });

        return new StreamBatch<>(batch, new GetReadCursor(largestEventId.get() + 1));
    }

}
