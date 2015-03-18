package com.jivesoftware.os.miru.wal.activity;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.Sip;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.partition.MiruPartitionCursor;
import java.util.Collection;

/** @author jonathan */
public interface MiruActivityWALReader {

    interface StreamMiruActivityWAL {

        boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception;
    }

    void stream(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        byte sortOrder,
        long afterTimestamp,
        int batchSize,
        StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception;

    void streamSip(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        byte sortOrder,
        Sip afterSip,
        int batchSize,
        StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception;

    MiruPartitionCursor getCursorForWriterId(MiruTenantId tenantId, int writerId, int desiredPartitionCapacity) throws Exception;

    MiruActivityWALStatus getStatus(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    long countSip(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    MiruPartitionedActivity findExisting(MiruTenantId tenantId, MiruPartitionId partitionId, MiruPartitionedActivity activity) throws Exception;

    long oldestActivityClockTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    void delete(MiruTenantId tenantId, MiruPartitionId partitionId, Collection<MiruActivityWALColumnKey> keys) throws Exception;

    void deleteSip(MiruTenantId tenantId, MiruPartitionId partitionId, Collection<MiruActivitySipWALColumnKey> keys) throws Exception;

}
