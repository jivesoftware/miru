package com.jivesoftware.os.miru.cluster;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.rcvs.MiruActivityLookupEntry;
import com.jivesoftware.os.miru.cluster.rcvs.MiruVersionedActivityLookupEntry;
import java.util.List;

/**
 *
 */
public interface MiruActivityLookupTable {

    MiruVersionedActivityLookupEntry getVersionedEntry(MiruTenantId tenantId, long activityTimestamp) throws Exception;

    void add(MiruTenantId tenantId, List<MiruPartitionedActivity> activities) throws Exception;

    void stream(MiruTenantId tenantId, long afterTimestamp, StreamLookupEntry streamLookupEntry) throws Exception;

    public interface StreamLookupEntry {
        boolean stream(long activityTimestamp, MiruActivityLookupEntry entry, long version) throws Exception;
    }
}
