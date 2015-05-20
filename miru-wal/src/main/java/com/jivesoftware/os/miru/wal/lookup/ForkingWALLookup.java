package com.jivesoftware.os.miru.wal.lookup;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import java.util.List;
import java.util.Map;

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
    public List<MiruTenantId> allTenantIds() throws Exception {
        return primaryLookup.allTenantIds();
    }

    @Override
    public MiruVersionedActivityLookupEntry[] getVersionedEntries(MiruTenantId tenantId, Long[] activityTimestamp) throws Exception {
        return primaryLookup.getVersionedEntries(tenantId, activityTimestamp);
    }

    @Override
    public Map<MiruPartitionId, RangeMinMax> add(MiruTenantId tenantId, List<MiruPartitionedActivity> activities) throws Exception {
        Map<MiruPartitionId, RangeMinMax> partitionMinMax = primaryLookup.add(tenantId, activities);
        secondaryLookup.add(tenantId, activities);
        return partitionMinMax;
    }

    @Override
    public void stream(MiruTenantId tenantId, long afterTimestamp, StreamLookupEntry streamLookupEntry) throws Exception {
        primaryLookup.stream(tenantId, afterTimestamp, streamLookupEntry);
    }
}
