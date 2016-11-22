package com.jivesoftware.os.miru.wal.activity;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.WriterCursor;
import com.jivesoftware.os.miru.wal.lookup.PartitionsStream;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.util.List;
import java.util.Set;

public class ForkingActivityWALReader<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> implements MiruActivityWALReader<C, S> {

    private final MiruActivityWALReader<C, S> readWAL;
    private final MiruActivityWALReader<?, ?> routingWAL;

    public ForkingActivityWALReader(MiruActivityWALReader<C, S> readWAL, MiruActivityWALReader<?, ?> routingWAL) {
        this.readWAL = readWAL;
        this.routingWAL = routingWAL;
    }

    @Override
    public HostPort[] getRoutingGroup(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return routingWAL.getRoutingGroup(tenantId, partitionId);
    }

    @Override
    public C stream(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        C afterCursor,
        int batchSize,
        long stopAtTimestamp,
        StreamMiruActivityWAL streamMiruActivityWAL) throws Exception {
        return readWAL.stream(tenantId, partitionId, afterCursor, batchSize, stopAtTimestamp, streamMiruActivityWAL);
    }

    @Override
    public S streamSip(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        S afterSipCursor,
        Set<TimeAndVersion> lastSeen,
        int batchSize,
        StreamMiruActivityWAL streamMiruActivityWAL,
        StreamSuppressed streamSuppressed) throws Exception {
        return readWAL.streamSip(tenantId, partitionId, afterSipCursor, lastSeen, batchSize, streamMiruActivityWAL, streamSuppressed);
    }

    @Override
    public WriterCursor getCursorForWriterId(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) throws Exception {
        return readWAL.getCursorForWriterId(tenantId, partitionId, writerId);
    }

    @Override
    public MiruActivityWALStatus getStatus(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return readWAL.getStatus(tenantId, partitionId);
    }

    @Override
    public long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return readWAL.oldestActivityClockTimestamp(tenantId, partitionId);
    }

    @Override
    public List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, MiruPartitionId partitionId, Long[] timestamps) throws Exception {
        return readWAL.getVersionedEntries(tenantId, partitionId, timestamps);
    }

    @Override
    public void allPartitions(PartitionsStream partitionsStream) throws Exception {
        readWAL.allPartitions(partitionsStream);
    }

    @Override
    public long clockMax(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return readWAL.clockMax(tenantId, partitionId);
    }
}
