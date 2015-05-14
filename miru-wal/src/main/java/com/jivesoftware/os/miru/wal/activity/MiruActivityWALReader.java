package com.jivesoftware.os.miru.wal.activity;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.partition.MiruPartitionCursor;
import java.util.Collection;

/** @author jonathan */
public interface MiruActivityWALReader<C, S> {

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

    MiruPartitionCursor getCursorForWriterId(MiruTenantId tenantId, int writerId, int desiredPartitionCapacity) throws Exception;

    MiruActivityWALStatus getStatus(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    void delete(MiruTenantId tenantId, MiruPartitionId partitionId, Collection<MiruActivityWALColumnKey> keys) throws Exception;

    void deleteSip(MiruTenantId tenantId, MiruPartitionId partitionId, Collection<MiruActivitySipWALColumnKey> keys) throws Exception;

    void allPartitions(PartitionsStream stream) throws Exception;

    interface StreamMiruActivityWAL {

        boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception;
    }

    interface PartitionsStream {

        boolean stream(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;
    }
}
