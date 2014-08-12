package com.jivesoftware.os.miru.wal.activity.naive;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import java.util.List;

/**
 *
 */
public class MiruNoOpActivityWAL implements MiruActivityWALReader, MiruActivityWALWriter {

    @Override
    public void stream(MiruTenantId tenantId, MiruPartitionId partitionId, long afterTimestamp, StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception {
    }

    @Override
    public void streamSip(MiruTenantId tenantId, MiruPartitionId partitionId, long afterTimestamp, StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception {
    }

    @Override
    public void write(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
    }

    @Override
    public MiruPartitionedActivity findExisting(MiruTenantId tenantId, MiruPartitionId partitionId, MiruPartitionedActivity activity) throws Exception {
        return null;
    }
}
