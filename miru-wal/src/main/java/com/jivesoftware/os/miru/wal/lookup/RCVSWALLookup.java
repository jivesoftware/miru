package com.jivesoftware.os.miru.wal.lookup;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.HostPortProvider;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.MiruVoidByte;
import com.jivesoftware.os.miru.api.wal.MiruActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.timestamper.ConstantTimestamper;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class RCVSWALLookup implements MiruWALLookup {

    private final HostPortProvider hostPortProvider;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> activityLookupTable;

    public RCVSWALLookup(HostPortProvider hostPortProvider,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> activityLookupTable) {
        this.hostPortProvider = hostPortProvider;
        this.activityLookupTable = activityLookupTable;
    }

    @Override
    public HostPort[] getRoutingGroup(MiruTenantId tenantId) throws Exception {
        RowColumnValueStore.HostAndPort hostAndPort = activityLookupTable.locate(MiruVoidByte.INSTANCE, tenantId);
        int port = hostPortProvider.getPort(hostAndPort.host);
        if (port < 0) {
            return new HostPort[0];
        } else {
            return new HostPort[] { new HostPort(hostAndPort.host, port) };
        }
    }

    @Override
    public List<MiruVersionedActivityLookupEntry> getVersionedEntries(MiruTenantId tenantId, final Long[] activityTimestamps) throws Exception {

        ColumnValueAndTimestamp<Long, MiruActivityLookupEntry, Long>[] got = activityLookupTable.multiGetEntries(MiruVoidByte.INSTANCE,
            tenantId, activityTimestamps, null, null);

        MiruVersionedActivityLookupEntry[] entrys = new MiruVersionedActivityLookupEntry[activityTimestamps.length];
        for (int i = 0; i < entrys.length; i++) {
            if (got[i] == null) {
                entrys[i] = null;
            } else {
                entrys[i] = new MiruVersionedActivityLookupEntry(activityTimestamps[i],
                    got[i].getTimestamp(),
                    got[i].getValue());
            }
        }
        return Arrays.asList(entrys);
    }

    @Override
    public void add(MiruTenantId tenantId, List<MiruVersionedActivityLookupEntry> entries) throws Exception {
        for (MiruVersionedActivityLookupEntry versionedEntry : entries) {
            activityLookupTable.add(MiruVoidByte.INSTANCE, tenantId, versionedEntry.timestamp, versionedEntry.entry, null,
                new ConstantTimestamper(versionedEntry.version));
        }
    }

    @Override
    public void stream(MiruTenantId tenantId, long afterTimestamp, final StreamLookupEntry streamLookupEntry) throws Exception {
        activityLookupTable.getEntrys(MiruVoidByte.INSTANCE, tenantId, afterTimestamp, Long.MAX_VALUE, 1_000, false, null, null,
            (ColumnValueAndTimestamp<Long, MiruActivityLookupEntry, Long> v) -> {
                if (v != null) {
                    if (!streamLookupEntry.stream(v.getColumn(), v.getValue(), v.getTimestamp())) {
                        return null;
                    }
                }
                return v;
            });
    }

    @Override
    public List<MiruTenantId> allTenantIds() throws Exception {
        final List<MiruTenantId> tenantIds = Lists.newArrayList();
        activityLookupTable.getAllRowKeys(10_000, null, r -> {
            if (r != null) {
                tenantIds.add(r.getRow());
            }
            return r;
        });
        return tenantIds;
    }
}
