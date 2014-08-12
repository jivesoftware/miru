package com.jivesoftware.os.miru.wal.activity;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/** @author jonathan */
public interface MiruActivityWALReader {

    interface StreamMiruActivityWAL {

        boolean stream(long collisionId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception;
    }

    void stream(MiruTenantId tenantId, MiruPartitionId partitionId, long afterTimestamp, StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception;

    void streamSip(MiruTenantId tenantId, MiruPartitionId partitionId, long afterTimestamp, StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception;

    MiruPartitionedActivity findExisting(MiruTenantId tenantId, MiruPartitionId partitionId, MiruPartitionedActivity activity) throws Exception;

}
