package com.jivesoftware.os.miru.writer.partition;

import com.google.common.base.Charsets;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.service.AmzaPartitionUpdates;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider.CheckOnline;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider.EmbeddedClient;
import com.jivesoftware.os.amza.service.Partition.ScanRange;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.WriterCursor;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jonathan.colt
 */
public class AmzaPartitionIdProvider implements MiruPartitionIdProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final byte[] AMZA_RING_NAME = "partitionIds".getBytes(Charsets.UTF_8);
    public static final PartitionName LATEST_PARTITIONS_PARTITION_NAME = new PartitionName(false,
        AMZA_RING_NAME,
        "latestPartitions".getBytes(Charsets.UTF_8));
    public static final PartitionName CURSORS_PARTITION_NAME = new PartitionName(false,
        AMZA_RING_NAME,
        "cursors".getBytes(Charsets.UTF_8));

    private final AmzaService amzaService;
    private final EmbeddedClientProvider clientProvider;
    private final long replicateLatestPartitionTimeoutMillis;
    private final long replicateCursorTimeoutMillis;
    private final String indexClass;
    private final int capacity;
    private final MiruWALClient<?, ?> walClient;
    private final AtomicBoolean ringInitialized = new AtomicBoolean(false);

    public AmzaPartitionIdProvider(AmzaService amzaService,
        EmbeddedClientProvider clientProvider,
        long replicateLatestPartitionTimeoutMillis,
        long replicateCursorTimeoutMillis,
        String indexClass,
        int capacity,
        MiruWALClient<?, ?> walClient) {
        this.amzaService = amzaService;
        this.clientProvider = clientProvider;
        this.replicateLatestPartitionTimeoutMillis = replicateLatestPartitionTimeoutMillis;
        this.replicateCursorTimeoutMillis = replicateCursorTimeoutMillis;
        this.indexClass = indexClass;
        this.capacity = capacity;
        this.walClient = walClient;
    }

    private byte[] key(MiruTenantId tenantId, int writerId) {
        byte[] rawTenantBytes = tenantId.getBytes();
        ByteBuffer bb = ByteBuffer.allocate(rawTenantBytes.length + 4);
        bb.put(rawTenantBytes);
        bb.putInt(writerId);
        return bb.array();
    }

    //TODO replace this and key() with a marshaller
    public static MiruTenantId extractTenantForLatestPartition(WALKey key) {
        byte[] keyBytes = key.key;
        int length = keyBytes.length - 4;
        byte[] tenantBytes = new byte[length];
        System.arraycopy(keyBytes, 0, tenantBytes, 0, length);
        return new MiruTenantId(tenantBytes);
    }

    private EmbeddedClient ensureClient(PartitionName partitionName, Consistency consistency, boolean requireConsistency) throws Exception {
        if (!ringInitialized.get()) {
            amzaService.getRingWriter().ensureMaximalRing(AMZA_RING_NAME, 10_000L); //TODO config
            ringInitialized.set(true);
        }

        amzaService.createPartitionIfAbsent(partitionName, new PartitionProperties(Durability.fsync_async,
            0, 0, 0, 0, 0, 0, 0, 0, false,
            consistency,
            requireConsistency,
            true,
            false,
            RowType.primary,
            indexClass,
            8,
            null,
            -1,
            -1));
        amzaService.awaitOnline(partitionName, 10_000L); //TODO config
        return clientProvider.getClient(partitionName, CheckOnline.once);
    }

    private EmbeddedClient latestPartitionsClient() throws Exception {
        return ensureClient(LATEST_PARTITIONS_PARTITION_NAME, Consistency.none, false);
    }

    private EmbeddedClient cursorsClient() throws Exception {
        return ensureClient(CURSORS_PARTITION_NAME, Consistency.none, false);
    }

    @Override
    public MiruPartitionCursor getCursor(MiruTenantId tenantId, int writerId) throws Exception {
        byte[] key = key(tenantId, writerId);
        byte[] rawPartitionId = latestPartitionsClient().getValue(Consistency.none, null, key);
        if (rawPartitionId == null) {
            MiruPartitionId largestPartitionId = walClient.getLargestPartitionId(tenantId);
            WriterCursor cursor;
            if (largestPartitionId == null) {
                cursor = new WriterCursor(0, 0);
            } else {
                cursor = walClient.getCursorForWriterId(tenantId, largestPartitionId, writerId);
                if (cursor == null) {
                    throw new IllegalStateException("Unknown cursor for tenant " + tenantId + " writer " + writerId);
                }
            }
            LOG.debug("Recovered cursor for {} writer {} at partition {} index {}", tenantId, writerId, cursor.partitionId, cursor.index);
            MiruPartitionCursor cursorForWriterId = new MiruPartitionCursor(MiruPartitionId.of(cursor.partitionId), new AtomicInteger(cursor.index), capacity);
            return setCursor(tenantId, writerId, cursorForWriterId);
        } else {
            MiruPartitionId partitionId = MiruPartitionId.of(FilerIO.bytesInt(rawPartitionId));
            int latestIndex = getLatestIndex(tenantId, partitionId, writerId);
            return new MiruPartitionCursor(partitionId, new AtomicInteger(latestIndex), capacity);
        }
    }

    @Override
    public void saveCursor(MiruTenantId tenantId, MiruPartitionCursor cursor, int writerId) throws Exception {
        byte[] cursorKey = key(tenantId, writerId, cursor.getPartitionId());
        cursorsClient().commit(Consistency.none,
            null,
            new AmzaPartitionUpdates().set(cursorKey, FilerIO.intBytes(cursor.last()), -1),
            replicateCursorTimeoutMillis,
            TimeUnit.MILLISECONDS);
    }

    @Override
    public void rewindCursor(MiruTenantId tenantId, int writerId, int size) throws Exception {
        MiruPartitionCursor partitionCursor = getCursor(tenantId, writerId);
        int rewindIndex = Math.max(partitionCursor.last() - size, 0);
        setCursor(tenantId, writerId, new MiruPartitionCursor(partitionCursor.getPartitionId(), new AtomicInteger(rewindIndex), capacity));
    }

    private MiruPartitionCursor setCursor(MiruTenantId tenantId, int writerId, MiruPartitionCursor cursor) throws Exception {
        EmbeddedClient latestPartitions = latestPartitionsClient();
        byte[] latestPartitionKey = key(tenantId, writerId);
        byte[] latestPartitionValue = latestPartitions.getValue(Consistency.none, null, latestPartitionKey);
        if (latestPartitionValue == null || FilerIO.bytesInt(latestPartitionValue) < cursor.getPartitionId().getId()) {
            latestPartitions.commit(Consistency.none, null,
                new AmzaPartitionUpdates().set(latestPartitionKey, FilerIO.intBytes(cursor.getPartitionId().getId()), -1),
                replicateLatestPartitionTimeoutMillis, TimeUnit.MILLISECONDS);
        }
        byte[] cursorKey = key(tenantId, writerId, cursor.getPartitionId());
        cursorsClient().commit(Consistency.none, null,
            new AmzaPartitionUpdates().set(cursorKey, FilerIO.intBytes(cursor.last()), -1),
            replicateCursorTimeoutMillis,
            TimeUnit.MILLISECONDS);
        return cursor;
    }

    private byte[] key(MiruTenantId tenantId, int writerId, MiruPartitionId partitionId) {
        byte[] rawTenantBytes = tenantId.getBytes();
        ByteBuffer bb = ByteBuffer.allocate(rawTenantBytes.length + 4 + 4);
        bb.put(rawTenantBytes);
        bb.putInt(writerId);
        bb.putInt(partitionId.getId());
        return bb.array();
    }

    @Override
    public MiruPartitionCursor nextCursor(MiruTenantId tenantId, MiruPartitionCursor lastCursor, int writerId) throws Exception {
        MiruPartitionCursor currentCursor = getCursor(tenantId, writerId);
        if (currentCursor.getPartitionId().equals(lastCursor.getPartitionId())) {
            MiruPartitionId next = lastCursor.getPartitionId().next();
            MiruPartitionCursor nextCursor = new MiruPartitionCursor(next, new AtomicInteger(0), capacity);
            return setCursor(tenantId, writerId, nextCursor);
        } else {
            throw new RuntimeException("last cursor:" + lastCursor + " should be equals to current cursor:" + currentCursor);
        }
    }

    @Override
    public int getLatestIndex(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) throws Exception {
        EmbeddedClient cursors = cursorsClient();
        byte[] cursorKey = key(tenantId, writerId, partitionId);
        byte[] got = cursors.getValue(Consistency.none, null, cursorKey);
        if (got == null) {
            WriterCursor cursor = walClient.getCursorForWriterId(tenantId, partitionId, writerId);
            if (cursor == null) {
                throw new IllegalStateException("Unknown cursor for tenant " + tenantId + " partition " + partitionId + " writer " + writerId);
            }
            cursors.commit(Consistency.none,
                null,
                new AmzaPartitionUpdates().set(cursorKey, FilerIO.intBytes(cursor.index), -1),
                replicateCursorTimeoutMillis,
                TimeUnit.MILLISECONDS);
            return cursor.index;
        } else {
            return FilerIO.bytesInt(got);
        }
    }

    @Override
    public void setLargestPartitionIdForWriter(MiruTenantId tenantId, MiruPartitionId partition, int writerId, int index) throws Exception {
        MiruPartitionCursor cursor = new MiruPartitionCursor(partition, new AtomicInteger(index), capacity);
        setCursor(tenantId, writerId, cursor);
    }

    @Override
    public MiruPartitionId getLargestPartitionIdAcrossAllWriters(MiruTenantId tenantId) throws Exception {
        final AtomicInteger largestPartitionId = new AtomicInteger(0);
        byte[] from = tenantId.getBytes();
        byte[] to = WALKey.prefixUpperExclusive(from);
        latestPartitionsClient().scan(
            Collections.singletonList(new ScanRange(null, from, null, to)),
            (prefix, key, value, timestamp, version) -> {
                int partitionId = FilerIO.bytesInt(value);
                if (largestPartitionId.get() < partitionId) {
                    largestPartitionId.set(partitionId);
                }
                return true;
            },
            true
        );
        return MiruPartitionId.of(largestPartitionId.get());
    }

}
