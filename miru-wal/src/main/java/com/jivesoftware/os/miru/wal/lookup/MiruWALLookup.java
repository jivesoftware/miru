package com.jivesoftware.os.miru.wal.lookup;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;
import java.util.concurrent.Callable;

/**
 *
 */
public interface MiruWALLookup {

    void add(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception;

    void markRepaired() throws Exception;

    List<MiruTenantId> allTenantIds(Callable<Void> repairCallback) throws Exception;

    void allPartitions(PartitionsStream partitionsStream, Callable<Void> repairCallback) throws Exception;

    void allPartitionsForTenant(MiruTenantId tenantId, PartitionsStream partitionsStream, Callable<Void> repairCallback) throws Exception;

    MiruPartitionId largestPartitionId(MiruTenantId tenantId, Callable<Void> repairCallback) throws Exception;

}
