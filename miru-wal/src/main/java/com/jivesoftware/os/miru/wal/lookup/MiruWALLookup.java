package com.jivesoftware.os.miru.wal.lookup;

import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.api.wal.MiruActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import java.util.List;
import java.util.Map;

/**
 *
 */
public interface MiruWALLookup {

    List<MiruTenantId> allTenantIds() throws Exception;

    MiruVersionedActivityLookupEntry[] getVersionedEntries(MiruTenantId tenantId, Long[] activityTimestamp) throws Exception;

    Map<MiruPartitionId, RangeMinMax> add(MiruTenantId tenantId, List<MiruPartitionedActivity> activities) throws Exception;

    void stream(MiruTenantId tenantId, long afterTimestamp, StreamLookupEntry streamLookupEntry) throws Exception;

    interface StreamLookupEntry {

        boolean stream(long activityTimestamp, MiruActivityLookupEntry entry, long version) throws Exception;
    }

}
