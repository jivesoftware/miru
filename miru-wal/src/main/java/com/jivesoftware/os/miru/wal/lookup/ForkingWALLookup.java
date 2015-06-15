package com.jivesoftware.os.miru.wal.lookup;

import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.routing.bird.shared.HostPort;
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
    public HostPort[] getRoutingGroup(MiruTenantId tenantId) throws Exception {
        return primaryLookup.getRoutingGroup(tenantId);
    }

    @Override
    public List<MiruTenantId> allTenantIds() throws Exception {
        return primaryLookup.allTenantIds();
    }

    @Override
    public List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, Long[] activityTimestamp) throws Exception {
        return primaryLookup.getVersionedEntries(tenantId, activityTimestamp);
    }

    @Override
    public void add(MiruTenantId tenantId, List<MiruVersionedActivityLookupEntry> entries) throws Exception {
        primaryLookup.add(tenantId, entries);
        secondaryLookup.add(tenantId, entries);
    }

    @Override
    public void stream(MiruTenantId tenantId, long afterTimestamp, StreamLookupEntry streamLookupEntry) throws Exception {
        primaryLookup.stream(tenantId, afterTimestamp, streamLookupEntry);
    }
}
