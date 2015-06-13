package com.jivesoftware.os.miru.wal.lookup;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.util.List;

/**
 *
 */
public interface MiruWALLookup {

    HostPort[] getRoutingGroup(MiruTenantId tenantId) throws Exception;

    List<MiruTenantId> allTenantIds() throws Exception;

    List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, Long[] activityTimestamp) throws Exception;

    void add(MiruTenantId tenantId, List<MiruVersionedActivityLookupEntry> entries) throws Exception;

    void stream(MiruTenantId tenantId, long afterTimestamp, StreamLookupEntry streamLookupEntry) throws Exception;

    interface StreamLookupEntry {

        boolean stream(long activityTimestamp, MiruActivityLookupEntry entry, long version) throws Exception;
    }

}
