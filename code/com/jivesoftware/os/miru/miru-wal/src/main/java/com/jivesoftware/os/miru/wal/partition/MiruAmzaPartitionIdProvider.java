/*
 * Copyright 2015 jonathan.colt.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.miru.wal.partition;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.AmzaTable;
import com.jivesoftware.os.amza.shared.RowIndexKey;
import com.jivesoftware.os.amza.shared.RowIndexValue;
import com.jivesoftware.os.amza.shared.RowScan;
import com.jivesoftware.os.amza.shared.TableName;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author jonathan.colt
 */
public class MiruAmzaPartitionIdProvider implements MiruPartitionIdProvider {

    private final AmzaService amzaService;
    private final AmzaTable latestPartitions;
    private final AmzaTable cursors;
    private final int capacity;

    public MiruAmzaPartitionIdProvider(AmzaService amzaService, int capacity) throws Exception {
        this.amzaService = amzaService;
        this.latestPartitions = amzaService.getTable(new TableName("master", "latestPartitions", null, null));
        this.cursors = amzaService.getTable(new TableName("master", "cursors", null, null));
        this.capacity = capacity;
    }

    @Override
    public Optional<MiruPartitionId> getLatestPartitionIdForTenant(MiruTenantId tenantId) throws Exception {
        throw new UnsupportedOperationException("Not yet implemented!!!.");
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
            return setCursor(key, tenantId, writerId, 0, 0);
        } else {
            int[] rawPartitonIdAndIndex = FilerIO.bytesInts(rawPartitonId);
            return new MiruPartitionCursor(MiruPartitionId.of(rawPartitonIdAndIndex[0]), new AtomicInteger(rawPartitonIdAndIndex[1]), capacity);
        }
    }

    private MiruPartitionCursor setCursor(RowIndexKey latestPartitionKey, MiruTenantId tenantId,
        int writerId, int rawPartitionId, int rawIndex) throws Exception {
        latestPartitions.set(latestPartitionKey, FilerIO.intBytes(rawPartitionId));
        RowIndexKey cursorKey = new RowIndexKey(key(tenantId, writerId, MiruPartitionId.of(rawPartitionId)));
        cursors.set(cursorKey, FilerIO.intBytes(rawIndex));
        return new MiruPartitionCursor(MiruPartitionId.of(rawPartitionId), new AtomicInteger(rawIndex), capacity);
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
            RowIndexKey latestPartitionKey = new RowIndexKey(key(tenantId, writerId));
            MiruPartitionId next = lastCursor.getPartitionId().next();
            return setCursor(latestPartitionKey, tenantId, writerId, next.getId(), 0);
        } else {
            throw new RuntimeException("last cursor:" + lastCursor + " should be equals to current cursor:" + currentCursor);
        }
    }

    @Override
    public int getLatestIndex(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) throws Exception {
        RowIndexKey cursorKey = new RowIndexKey(key(tenantId, writerId, partitionId));
        byte[] got = cursors.get(cursorKey);
        if (got == null) {
            return -1;
        } else {
            return FilerIO.bytesInt(got);
        }
    }

    @Override
    public void setLargestPartitionIdForWriter(MiruTenantId tenantId, MiruPartitionId partition, int writerId) throws Exception {
        RowIndexKey latestPartitionKey = new RowIndexKey(key(tenantId, writerId));
        setCursor(latestPartitionKey, tenantId, writerId, partition.getId(), 0);
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
