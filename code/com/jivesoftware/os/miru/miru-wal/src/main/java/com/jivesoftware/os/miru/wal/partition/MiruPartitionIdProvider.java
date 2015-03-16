package com.jivesoftware.os.miru.wal.partition;

import com.google.common.base.Optional;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 * This call allows all writes to get the must latest partition id across all writers.
 *
 * @author jonathan
 */
public interface MiruPartitionIdProvider {

    Optional<MiruPartitionId> getLatestPartitionIdForTenant(MiruTenantId tenantId) throws Exception;

    MiruPartitionCursor getCursor(MiruTenantId tenantId, int writerId) throws Exception;

    MiruPartitionCursor nextCursor(MiruTenantId tenantId, MiruPartitionCursor lastCursor, int writerId) throws Exception;

    int getLatestIndex(MiruTenantId tenantId, MiruPartitionId partitionId, int writerId) throws Exception;

    void setLargestPartitionIdForWriter(MiruTenantId tenantId, MiruPartitionId partition, int writerId) throws Exception;

    MiruPartitionId getLargestPartitionIdAcrossAllWriters(MiruTenantId tenantId) throws Exception;
}
