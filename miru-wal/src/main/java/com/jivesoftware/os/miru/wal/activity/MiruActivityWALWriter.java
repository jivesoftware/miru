package com.jivesoftware.os.miru.wal.activity;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import java.util.Collection;
import java.util.List;

/**
 * @author jonathan
 */
public interface MiruActivityWALWriter {

    RangeMinMax write(MiruTenantId tenantId, MiruPartitionId partitionId, List<MiruPartitionedActivity> partitionedActivities) throws Exception;

    void delete(MiruTenantId tenantId, MiruPartitionId partitionId, Collection<MiruActivityWALColumnKey> keys) throws Exception;

    void deleteSip(MiruTenantId tenantId, MiruPartitionId partitionId, Collection<MiruActivitySipWALColumnKey> keys) throws Exception;

    void removePartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;
}
