package com.jivesoftware.os.miru.wal.lookup;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.client.AmzaKretrProvider.AmzaKretr;
import com.jivesoftware.os.amza.shared.AmzaPartitionAPI.TimestampedValue;
import com.jivesoftware.os.amza.shared.AmzaPartitionUpdates;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.miru.wal.AmzaWALUtil;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AmzaWALLookup implements MiruWALLookup {

    private final AmzaWALUtil amzaWALUtil;
    private final int replicateLookupQuorum;
    private final long replicateLookupTimeoutMillis;
    private final MiruActivityLookupEntryMarshaller activityLookupEntryMarshaller = new MiruActivityLookupEntryMarshaller();
    private final Set<MiruTenantId> knownLookupTenants = Collections.newSetFromMap(Maps.newConcurrentMap());

    public AmzaWALLookup(AmzaWALUtil amzaWALUtil,
        int replicateLookupQuorum,
        long replicateLookupTimeoutMillis) {
        this.amzaWALUtil = amzaWALUtil;
        this.replicateLookupQuorum = replicateLookupQuorum;
        this.replicateLookupTimeoutMillis = replicateLookupTimeoutMillis;
    }

    @Override
    public HostPort[] getRoutingGroup(MiruTenantId tenantId) throws Exception {
        return amzaWALUtil.getLookupRoutingGroup(tenantId);
    }

    @Override
    public List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, final Long[] activityTimestamps) throws Exception {
        AmzaKretr client = amzaWALUtil.getLookupClient(tenantId);

        MiruVersionedActivityLookupEntry[] entries = new MiruVersionedActivityLookupEntry[activityTimestamps.length];
        for (int i = 0; i < activityTimestamps.length; i++) {
            WALKey key = new WALKey(FilerIO.longBytes(activityTimestamps[i]));
            TimestampedValue value = client.getTimestampedValue(key);
            if (value != null) {
                entries[i] = new MiruVersionedActivityLookupEntry(activityTimestamps[i],
                    value.getTimestampId(),
                    activityLookupEntryMarshaller.fromBytes(value.getValue()));
            }
        }
        return Arrays.asList(entries);
    }

    @Override
    public void add(MiruTenantId tenantId, List<MiruVersionedActivityLookupEntry> entries) throws Exception {
        if (!knownLookupTenants.contains(tenantId)) {
            AmzaKretr lookupTenantsRegion = amzaWALUtil.getLookupTenantsClient();
            lookupTenantsRegion.commit(new AmzaPartitionUpdates().set(new WALKey(tenantId.getBytes()), null), 1, 1, TimeUnit.MINUTES); //TODO config
            knownLookupTenants.add(tenantId);
        }

        AmzaKretr lookupActivityRegion = amzaWALUtil.getLookupClient(tenantId);

        for (MiruVersionedActivityLookupEntry versionedEntry : entries) {
            lookupActivityRegion.commit(
                new AmzaPartitionUpdates().set(new WALKey(FilerIO.longBytes(versionedEntry.timestamp)),
                    activityLookupEntryMarshaller.toBytes(versionedEntry.entry), versionedEntry.version),
                replicateLookupQuorum,
                replicateLookupTimeoutMillis,
                TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void stream(MiruTenantId tenantId, long afterTimestamp, final StreamLookupEntry streamLookupEntry) throws Exception {
        AmzaKretr client = amzaWALUtil.getLookupClient(tenantId);

        if (client != null) {
            client.scan(new WALKey(FilerIO.longBytes(afterTimestamp)), null, (rowTxId, key, value) -> {
                if (value != null) {
                    MiruActivityLookupEntry entry = activityLookupEntryMarshaller.fromBytes(value.getValue());
                    if (!streamLookupEntry.stream(FilerIO.bytesLong(key.getKey()), entry, value.getTimestampId())) {
                        return false;
                    }
                }
                return true;
            });
        }
    }

    @Override
    public List<MiruTenantId> allTenantIds() throws Exception {
        AmzaKretr client = amzaWALUtil.getLookupTenantsClient();

        final List<MiruTenantId> tenantIds = Lists.newArrayList();
        if (client != null) {
            client.scan(null, null, (rowTxId, key, value) -> {
                if (key != null) {
                    tenantIds.add(new MiruTenantId(key.getKey()));
                }
                return true;
            });
        }
        return tenantIds;
    }

}
