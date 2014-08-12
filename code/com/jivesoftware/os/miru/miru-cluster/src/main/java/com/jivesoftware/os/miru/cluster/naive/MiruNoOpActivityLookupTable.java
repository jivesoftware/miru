package com.jivesoftware.os.miru.cluster.naive;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruActivityLookupTable;
import com.jivesoftware.os.miru.cluster.rcvs.MiruVersionedActivityLookupEntry;
import java.util.List;

/**
 *
 */
public class MiruNoOpActivityLookupTable implements MiruActivityLookupTable {

    @Override
    public MiruVersionedActivityLookupEntry getVersionedEntry(MiruTenantId tenantId, long activityTimestamp) throws Exception {
        return null;
    }

    @Override
    public void add(MiruTenantId tenantId, List<MiruPartitionedActivity> activities) throws Exception {
    }

    @Override
    public void stream(MiruTenantId tenantId, long afterTimestamp, StreamLookupEntry streamLookupEntry) throws Exception {
    }
}
