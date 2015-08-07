package com.jivesoftware.os.miru.wal.lookup;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.List;
import java.util.concurrent.Callable;

/**
 *
 */
public class ForkingWALLookup implements MiruWALLookup {

    private final MiruWALLookup primaryLookup;
    private final MiruWALLookup secondaryLookup;

    public ForkingWALLookup(MiruWALLookup primaryLookup, MiruWALLookup secondaryLookup) {
        this.primaryLookup = primaryLookup;
        this.secondaryLookup = secondaryLookup;
    }

    @Override
    public void add(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        primaryLookup.add(tenantId, partitionId);
        secondaryLookup.add(tenantId, partitionId);
    }

    @Override
    public void markRepaired() throws Exception {
        primaryLookup.markRepaired();
        secondaryLookup.markRepaired();
    }

    @Override
    public List<MiruTenantId> allTenantIds(Callable<Void> repairCallback) throws Exception {
        return primaryLookup.allTenantIds(repairCallback);
    }

    @Override
    public void allPartitions(PartitionsStream partitionsStream, Callable<Void> repairCallback) throws Exception {
        primaryLookup.allPartitions(partitionsStream, repairCallback);
    }

    @Override
    public void allPartitionsForTenant(MiruTenantId tenantId, PartitionsStream partitionsStream, Callable<Void> repairCallback) throws Exception {
        primaryLookup.allPartitionsForTenant(tenantId, partitionsStream, repairCallback);
    }

    @Override
    public MiruPartitionId largestPartitionId(MiruTenantId tenantId, Callable<Void> repairCallback) throws Exception {
        return primaryLookup.largestPartitionId(tenantId, repairCallback);
    }
}
