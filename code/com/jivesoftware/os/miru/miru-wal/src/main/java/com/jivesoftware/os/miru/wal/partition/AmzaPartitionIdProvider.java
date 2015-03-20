package com.jivesoftware.os.miru.wal.partition;

import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaTable;
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author jonathan.colt
 */
public class AmzaPartitionIdProvider implements MiruPartitionIdProvider {

    private final AmzaTable latestPartitions;
    private final AmzaTable cursors;
    private final int capacity;
    private final MiruActivityWALReader walReader;

    public AmzaPartitionIdProvider(AmzaService amzaService, int capacity, MiruActivityWALReader walReader) throws Exception {
        this.latestPartitions = amzaService.getTable(new TableName("master", "latestPartitions", null, null));
        this.cursors = amzaService.getTable(new TableName("master", "cursors", null, null));
        this.capacity = capacity;
        this.walReader = walReader;
    }

    private byte[] key(MiruTenantId tenantId, int writerId) {
        byte[] rawTenantBytes = tenantId.getBytes();
        ByteBuffer bb = ByteBuffer.allocate(rawTenantBytes.length + 4);
        bb.put(rawTenantBytes);
        bb.putInt(writerId);
        return bb.array();
    }

    @Override
    public MiruPartitionCursor getCursor(MiruTenantId tenantId, int writerId) throws Exception {
        RowIndexKey key = new RowIndexKey(key(tenantId, writerId));
        byte[] rawPartitonId = latestPartitions.get(key);
        if (rawPartitonId == null) {
            MiruPartitionCursor cursorForWriterId = walReader.getCursorForWriterId(tenantId, writerId, capacity);
            return setCursor(tenantId, writerId, cursorForWriterId);
        } else {
            MiruPartitionId partitionId = MiruPartitionId.of(FilerIO.bytesInt(rawPartitonId));
            int latestIndex = getLatestIndex(tenantId, partitionId, writerId);
            return new MiruPartitionCursor(partitionId, new AtomicInteger(latestIndex), capacity);
        }
    }

    private MiruPartitionCursor setCursor(MiruTenantId tenantId, int writerId, MiruPartitionCursor cursor) throws Exception {
        RowIndexKey latestPartitionKey = new RowIndexKey(key(tenantId, writerId));
        latestPartitions.set(latestPartitionKey, FilerIO.intBytes(cursor.getPartitionId().getId()));
        RowIndexKey cursorKey = new RowIndexKey(key(tenantId, writerId, cursor.getPartitionId()));
        cursors.set(cursorKey, FilerIO.intBytes(cursor.last()));
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
        RowIndexKey cursorKey = new RowIndexKey(key(tenantId, writerId, partitionId));
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
        latestPartitions.rangeScan(new RowIndexKey(rawTenantBytes), new RowIndexKey(prefixUpperExclusive(rawTenantBytes)), new RowScan<Exception>() {

            @Override
            public boolean row(long l, RowIndexKey rik, RowIndexValue riv) throws Exception {
                int partitionId = FilerIO.bytesInt(riv.getValue());
                if (largestPartitionId.get() < partitionId) {
                    largestPartitionId.set(partitionId);
                }
                return true;
            }
        });
        return MiruPartitionId.of(largestPartitionId.get());
    }

    public byte[] prefixUpperExclusive(byte[] preBytes) {
        byte[] raw = new byte[preBytes.length];
        System.arraycopy(preBytes, 0, raw, 0, preBytes.length);

        // given: [64,72,96,127]
        // want: [64,72,97,-128]
        for (int i = raw.length - 1; i >= 0; i--) {
            if (raw[i] == Byte.MAX_VALUE) {
                raw[i] = Byte.MIN_VALUE;
            } else {
                raw[i]++;
                break;
            }
        }
        return raw;
    }

}
