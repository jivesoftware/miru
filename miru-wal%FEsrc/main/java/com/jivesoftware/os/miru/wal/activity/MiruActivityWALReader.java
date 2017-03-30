package com.jivesoftware.os.miru.wal.activity;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
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

/** @author jonathan */
public interface MiruActivityWALReader<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> {

    HostPort[] getRoutingGroup(MiruTenantId tenantId, MiruPartitionId partitionId, boolean createIfAbsent) throws Exception;

    C stream(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        C afterCursor,
        int batchSize,
        long stopAtTimestamp,
        StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception;

    S streamSip(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        S afterSipCursor,
        Set<TimeAndVersion> lastSeen,
        int batchSize,
        StreamMiruActivityWAL streamMiruActivityWAL,
        StreamSuppressed streamSuppressed)
        throws Exception;

    WriterCursor getCursorForWriterId(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) throws Exception;

    MiruActivityWALStatus getStatus(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, MiruPartitionId partitionId, Long[] timestamps) throws Exception;

    void allPartitions(PartitionsStream partitionsStream) throws Exception;

    long clockMax(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    interface StreamMiruActivityWAL {

        boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception;
    }

    interface StreamSuppressed {

        void stream(TimeAndVersion timeAndVersion) throws Exception;
    }
}
