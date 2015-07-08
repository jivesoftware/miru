package com.jivesoftware.os.miru.wal.activity;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;

/**
 * @author jonathan
 */
public interface MiruActivityWALWriter {

    void write(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception;

    void copyPartition(MiruTenantId tenantId, MiruPartitionId from, MiruPartitionId to, int batchSize) throws Exception;

    void fixPartitionIds(MiruTenantId tenantId, MiruPartitionId partitionId, int batchSize) throws Exception;

    void removePartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;
}
