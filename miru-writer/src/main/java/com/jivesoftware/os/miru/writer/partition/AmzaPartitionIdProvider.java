package com.jivesoftware.os.miru.writer.partition;

import com.jivesoftware.os.amza.service.AmzaRegion;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.WriterCursor;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jonathan.colt
 */
public class AmzaPartitionIdProvider implements MiruPartitionIdProvider {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private static final String AMZA_RING_NAME = "partitionIds";
    public static final RegionName LATEST_PARTITIONS_REGION_NAME = new RegionName(false, AMZA_RING_NAME, "latestPartitions");
    public static final RegionName CURSORS_REGION_NAME = new RegionName(false, AMZA_RING_NAME, "cursors");

    private final AmzaService amzaService;
    private final WALStorageDescriptor amzaStorageDescriptor;
    private final int capacity;
    private final MiruWALClient<?, ?> walClient;
    private final AtomicBoolean ringInitialized = new AtomicBoolean(false);

    public AmzaPartitionIdProvider(AmzaService amzaService,
        WALStorageDescriptor amzaStorageDescriptor,
        int capacity,
        MiruWALClient<?, ?> walClient)
        throws Exception {
        this.amzaService = amzaService;
        this.amzaStorageDescriptor = amzaStorageDescriptor;
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
        byte[] keyBytes = key.getKey();
        int length = keyBytes.length - 4;
        byte[] tenantBytes = new byte[length];
        System.arraycopy(keyBytes, 0, tenantBytes, 0, length);
        return new MiruTenantId(tenantBytes);
    }

    private AmzaRegion createRegionIfAbsent(RegionName regionName) throws Exception {
        if (!ringInitialized.get()) {
            int ringSize = amzaService.getAmzaRing().getRingSize(AMZA_RING_NAME);
            int systemRingSize = amzaService.getAmzaRing().getRingSize("system");
            if (ringSize < systemRingSize) {
                amzaService.getAmzaRing().buildRandomSubRing(AMZA_RING_NAME, systemRingSize);
            }
            ringInitialized.set(true);
        }

        return amzaService.createRegionIfAbsent(regionName,
            new RegionProperties(amzaStorageDescriptor, 1, 1, false) //TODO config?
        );
    }

    @Override
    public MiruPartitionCursor getCursor(MiruTenantId tenantId, int writerId) throws Exception {
        WALKey key = new WALKey(key(tenantId, writerId));
        AmzaRegion latestPartitions = createRegionIfAbsent(LATEST_PARTITIONS_REGION_NAME);
        byte[] rawPartitonId = latestPartitions.get(key);
        if (rawPartitonId == null) {
            WriterCursor cursor = walClient.getCursorForWriterId(tenantId, writerId);
            if (cursor == null) {
                throw new IllegalStateException("Unknown cursor for tenant " + tenantId + " writer " + writerId);
            }
            LOG.debug("Recovered cursor for {} writer {} at partition {} index {}", tenantId, writerId, cursor.partitionId, cursor.index);
            MiruPartitionCursor cursorForWriterId = new MiruPartitionCursor(MiruPartitionId.of(cursor.partitionId), new AtomicInteger(cursor.index), capacity);
            return setCursor(tenantId, writerId, cursorForWriterId);
        } else {
            MiruPartitionId partitionId = MiruPartitionId.of(FilerIO.bytesInt(rawPartitonId));
            int latestIndex = getLatestIndex(tenantId, partitionId, writerId);
            return new MiruPartitionCursor(partitionId, new AtomicInteger(latestIndex), capacity);
        }
    }

    @Override
    public void saveCursor(MiruTenantId tenantId, MiruPartitionCursor cursor, int writerId) throws Exception {
        WALKey cursorKey = new WALKey(key(tenantId, writerId, cursor.getPartitionId()));
        createRegionIfAbsent(CURSORS_REGION_NAME).set(cursorKey, FilerIO.intBytes(cursor.last()));
    }

    private MiruPartitionCursor setCursor(MiruTenantId tenantId, int writerId, MiruPartitionCursor cursor) throws Exception {
        WALKey latestPartitionKey = new WALKey(key(tenantId, writerId));
        AmzaRegion latestPartitions = createRegionIfAbsent(LATEST_PARTITIONS_REGION_NAME);
        byte[] latestPartitionBytes = latestPartitions.get(latestPartitionKey);
        if (latestPartitionBytes == null || FilerIO.bytesInt(latestPartitionBytes) < cursor.getPartitionId().getId()) {
            latestPartitions.set(latestPartitionKey, FilerIO.intBytes(cursor.getPartitionId().getId()));
        }
        WALKey cursorKey = new WALKey(key(tenantId, writerId, cursor.getPartitionId()));
        createRegionIfAbsent(CURSORS_REGION_NAME).set(cursorKey, FilerIO.intBytes(cursor.last()));
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
        WALKey cursorKey = new WALKey(key(tenantId, writerId, partitionId));
        AmzaRegion cursors = createRegionIfAbsent(CURSORS_REGION_NAME);
        byte[] got = cursors.get(cursorKey);
        if (got == null) {
            cursors.set(cursorKey, FilerIO.intBytes(0));
            return 0;
        } else {
            return FilerIO.bytesInt(got);
        }
    }

    @Override
    public void setLargestPartitionIdForWriter(MiruTenantId tenantId, MiruPartitionId partition, int writerId) throws Exception {
        MiruPartitionCursor cursor = new MiruPartitionCursor(partition, new AtomicInteger(0), capacity);
        setCursor(tenantId, writerId, cursor);
    }

    @Override
    public MiruPartitionId getLargestPartitionIdAcrossAllWriters(MiruTenantId tenantId) throws Exception {
        byte[] rawTenantBytes = tenantId.getBytes();
        final AtomicInteger largestPartitionId = new AtomicInteger(0);
        WALKey from = new WALKey(rawTenantBytes);
        createRegionIfAbsent(LATEST_PARTITIONS_REGION_NAME).rangeScan(from, from.prefixUpperExclusive(), (rowTxId, key, value) -> {
            int partitionId = FilerIO.bytesInt(value.getValue());
            if (largestPartitionId.get() < partitionId) {
                largestPartitionId.set(partitionId);
            }
            return true;
        });
        return MiruPartitionId.of(largestPartitionId.get());
    }

}
