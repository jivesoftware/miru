package com.jivesoftware.os.miru.wal.lookup;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

/**
 *
 */
public interface PartitionsStream {

    boolean stream(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;
}
