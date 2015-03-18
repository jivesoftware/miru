package com.jivesoftware.os.miru.wal.lookup;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityLookupEntry;
import java.util.List;

/**
 *
 */
public interface MiruActivityLookupTable {

    MiruVersionedActivityLookupEntry[] getVersionedEntries(MiruTenantId tenantId, Long[] activityTimestamp) throws Exception;

    void add(MiruTenantId tenantId, List<MiruPartitionedActivity> activities) throws Exception;

    void stream(MiruTenantId tenantId, long afterTimestamp, StreamLookupEntry streamLookupEntry) throws Exception;

    List<MiruTenantId> allTenantIds() throws Exception;

    public interface StreamLookupEntry {

        boolean stream(long activityTimestamp, MiruActivityLookupEntry entry, long version) throws Exception;
    }
}
