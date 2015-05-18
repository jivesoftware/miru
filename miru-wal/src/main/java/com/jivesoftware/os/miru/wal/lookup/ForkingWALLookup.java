package com.jivesoftware.os.miru.wal.lookup;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import java.util.List;

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
    public void add(MiruTenantId tenantId, List<MiruPartitionedActivity> activities) throws Exception {
        primaryLookup.add(tenantId, activities);
        secondaryLookup.add(tenantId, activities);
    }

    @Override
    public void stream(MiruTenantId tenantId, long afterTimestamp, StreamLookupEntry streamLookupEntry) throws Exception {
        primaryLookup.stream(tenantId, afterTimestamp, streamLookupEntry);
    }

    @Override
    public void streamRanges(MiruTenantId tenantId, MiruPartitionId partitionId, StreamRangeLookup streamRangeLookup) throws Exception {
        primaryLookup.streamRanges(tenantId, partitionId, streamRangeLookup);
    }

    @Override
    public void putRange(MiruTenantId tenantId, MiruPartitionId partitionId, RangeMinMax minMax) throws Exception {
        primaryLookup.putRange(tenantId, partitionId, minMax);
        secondaryLookup.putRange(tenantId, partitionId, minMax);
    }
}
