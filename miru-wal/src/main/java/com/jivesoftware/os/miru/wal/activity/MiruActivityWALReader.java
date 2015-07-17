package com.jivesoftware.os.miru.wal.activity;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruWALClient.WriterCursor;
import com.jivesoftware.os.routing.bird.shared.HostPort;

/** @author jonathan */
public interface MiruActivityWALReader<C, S> {

    HostPort[] getRoutingGroup(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    C stream(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        C afterCursor,
        int batchSize,
        StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception;

    S streamSip(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        S afterSipCursor,
        int batchSize,
        StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception;

    WriterCursor getCursorForWriterId(MiruTenantId tenantId, int writerId) throws Exception;

    MiruActivityWALStatus getStatus(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    void allPartitions(PartitionsStream stream) throws Exception;

    MiruPartitionId largestPartitionId(MiruTenantId tenantId) throws Exception;

    interface StreamMiruActivityWAL {

        boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception;
    }

    interface PartitionsStream {

        boolean stream(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;
    }
}
