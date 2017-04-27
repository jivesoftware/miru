package com.jivesoftware.os.miru.wal;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.api.partition.PartitionName;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.ring.RingMember;
import com.jivesoftware.os.amza.api.stream.KeyValueTimestampStream;
import com.jivesoftware.os.amza.api.stream.TxKeyValueStream;
import com.jivesoftware.os.amza.api.take.TakeCursors;
import com.jivesoftware.os.amza.api.wal.WALKey;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider.CheckOnline;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider.EmbeddedClient;
import com.jivesoftware.os.amza.service.Partition.ScanRange;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import com.jivesoftware.os.miru.api.wal.AmzaCursor;
import com.jivesoftware.os.miru.wal.lookup.PartitionsStream;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class AmzaWALUtil {

    private static final PartitionName LOOKUP_TENANTS_PARTITION_NAME = new PartitionName(false,
        "lookup-tenants".getBytes(Charsets.UTF_8),
        "lookup-tenants".getBytes(Charsets.UTF_8));
    private static final PartitionName LOOKUP_PARTITIONS_PARTITION_NAME = new PartitionName(false,
        "lookup-partitions".getBytes(Charsets.UTF_8),
        "lookup-partitions".getBytes(Charsets.UTF_8));

    private final AmzaService amzaService;
    private final EmbeddedClientProvider clientProvider;
    private final PartitionProperties activityProperties;
    private final PartitionProperties readTrackingProperties;
    private final PartitionProperties lookupProperties;
    private final int activityRingSize;
    private final long activityRoutingTimeoutMillis;
    private final int readTrackingRingSize;
    private final long readTrackingRoutingTimeoutMillis;
    private final Map<PartitionName, EmbeddedClient> clientMap = Maps.newConcurrentMap();

    public AmzaWALUtil(AmzaService amzaService,
        EmbeddedClientProvider embeddedClientProvider,
        PartitionProperties activityProperties,
        PartitionProperties readTrackingProperties,
        PartitionProperties lookupProperties,
        int activityRingSize,
        long activityRoutingTimeoutMillis,
        int readTrackingRingSize,
        long readTrackingRoutingTimeoutMillis) {
        this.amzaService = amzaService;
        this.clientProvider = embeddedClientProvider;
        this.activityProperties = activityProperties;
        this.readTrackingProperties = readTrackingProperties;
        this.lookupProperties = lookupProperties;
        this.activityRingSize = activityRingSize;
        this.activityRoutingTimeoutMillis = activityRoutingTimeoutMillis;
        this.readTrackingRingSize = readTrackingRingSize;
        this.readTrackingRoutingTimeoutMillis = readTrackingRoutingTimeoutMillis;
    }

    public HostPort[] getActivityRoutingGroup(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        Optional<PartitionProperties> partitionProperties,
        boolean createIfAbsent) throws Exception {

        PartitionName partitionName = getActivityPartitionName(tenantId, partitionId);
        if (createIfAbsent) {
            amzaService.getRingWriter().ensureSubRing(partitionName.getRingName(), activityRingSize, activityRoutingTimeoutMillis);
            amzaService.createPartitionIfAbsent(partitionName, partitionProperties.or(activityProperties));
        } else if (!amzaService.hasPartition(partitionName)) {
            return new HostPort[0];
        }

        AmzaService.AmzaPartitionRoute partitionRoute = amzaService.getPartitionRoute(partitionName, activityRoutingTimeoutMillis);
        if (!partitionRoute.disposed && partitionRoute.leader != null) {
            return new HostPort[]{new HostPort(partitionRoute.leader.ringHost.getHost(), partitionRoute.leader.ringHost.getPort())};
        } else {
            return partitionRoute.orderedMembers.stream()
                .map(ringMemberAndHost -> new HostPort(ringMemberAndHost.ringHost.getHost(), ringMemberAndHost.ringHost.getPort()))
                .toArray(HostPort[]::new);
        }
    }

    public HostPort[] getReadTrackingRoutingGroup(MiruTenantId tenantId,
        Optional<PartitionProperties> partitionProperties,
        boolean createIfAbsent) throws Exception {

        PartitionName partitionName = getReadTrackingPartitionName(tenantId);
        if (createIfAbsent) {
            amzaService.getRingWriter().ensureSubRing(partitionName.getRingName(), readTrackingRingSize, readTrackingRoutingTimeoutMillis);
            amzaService.createPartitionIfAbsent(partitionName, partitionProperties.or(readTrackingProperties));
        } else if (!amzaService.hasPartition(partitionName)) {
            return new HostPort[0];
        }

        AmzaService.AmzaPartitionRoute partitionRoute = amzaService.getPartitionRoute(partitionName, readTrackingRoutingTimeoutMillis);
        if (!partitionRoute.disposed && partitionRoute.leader != null) {
            return new HostPort[]{new HostPort(partitionRoute.leader.ringHost.getHost(), partitionRoute.leader.ringHost.getPort())};
        } else {
            return partitionRoute.orderedMembers.stream()
                .map(ringMemberAndHost -> new HostPort(ringMemberAndHost.ringHost.getHost(), ringMemberAndHost.ringHost.getPort()))
                .toArray(HostPort[]::new);
        }
    }

    public void allActivityPartitions(PartitionsStream partitionsStream) throws Exception {
        byte[] prefix = "activityWAL-".getBytes(Charsets.UTF_8);

        partition:
        for (PartitionName partitionName : amzaService.getAllPartitionNames()) {
            byte[] nameBytes = partitionName.getName();
            if (nameBytes.length > prefix.length) {
                for (int i = 0; i < prefix.length; i++) {
                    if (nameBytes[i] != prefix[i]) {
                        continue partition;
                    }
                }
                String name = new String(nameBytes, Charsets.UTF_8);
                int firstHyphen = name.indexOf('-');
                int lastHyphen = name.lastIndexOf('-');
                if (!partitionsStream.stream(new MiruTenantId(name.substring(firstHyphen + 1, lastHyphen).getBytes(Charsets.UTF_8)),
                    MiruPartitionId.of(Integer.parseInt(name.substring(lastHyphen + 1))))) {
                    break;
                }
            }
        }
    }

    private PartitionName getActivityPartitionName(MiruTenantId tenantId, MiruPartitionId partitionId) {
        String walName = "activityWAL-" + tenantId.toString() + "-" + partitionId.toString();
        byte[] walNameBytes = walName.getBytes(Charsets.UTF_8);
        return new PartitionName(false, walNameBytes, walNameBytes);
    }

    private PartitionName getReadTrackingPartitionName(MiruTenantId tenantId) {
        String walName = "readTrackingWAL-" + tenantId.toString();
        byte[] walNameBytes = walName.getBytes(Charsets.UTF_8);
        return new PartitionName(false, walNameBytes, walNameBytes);
    }

    /* TODO should be done by the remote client looking up ordered hosts
    public AmzaClient getOrCreateClient(PartitionName partitionName, int ringSize, Optional<PartitionProperties> regionProperties) throws Exception {
        amzaService.getAmzaHostRing().ensureSubRing(partitionName.getRingName(), ringSize);
        amzaService.setPropertiesIfAbsent(partitionName, regionProperties.or(defaultProperties));
        return amzaClientProvider.getClient(partitionName);
    }
     */
    private EmbeddedClient getClient(PartitionName partitionName) throws Exception {
        return clientProvider.getClient(partitionName, CheckOnline.once);
    }

    public EmbeddedClient getActivityClient(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return getClient(getActivityPartitionName(tenantId, partitionId));
    }

    public EmbeddedClient getReadTrackingClient(MiruTenantId tenantId) throws Exception {
        return getClient(getReadTrackingPartitionName(tenantId));
    }

    public EmbeddedClient getLookupTenantsClient() throws Exception {
        return getOrCreateMaximalClient(LOOKUP_TENANTS_PARTITION_NAME, Optional.<PartitionProperties>absent());
    }

    public EmbeddedClient getLookupPartitionsClient() throws Exception {
        return getOrCreateMaximalClient(LOOKUP_PARTITIONS_PARTITION_NAME, Optional.<PartitionProperties>absent());
    }

    private EmbeddedClient getOrCreateMaximalClient(PartitionName partitionName, Optional<PartitionProperties> partitionProperties) throws Exception {
        return clientMap.computeIfAbsent(partitionName, key -> {
            try {
                amzaService.getRingWriter().ensureMaximalRing(partitionName.getRingName(), 10_000); //TODO config
                amzaService.createPartitionIfAbsent(partitionName, partitionProperties.or(lookupProperties));
                amzaService.awaitOnline(partitionName, 10_000); //TODO config
                return clientProvider.getClient(partitionName, CheckOnline.once);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create maximal client", e);
            }
        });
    }

    public boolean hasActivityPartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return amzaService.hasPartition(getActivityPartitionName(tenantId, partitionId));
    }

    public void destroyActivityPartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        amzaService.destroyPartition(getActivityPartitionName(tenantId, partitionId));
    }

    public long count(EmbeddedClient client, byte sort) throws Exception {
        long[] count = { 0 };
        byte[] fromKey = { sort };
        byte[] toKey = WALKey.prefixUpperExclusive(fromKey);
        client.scan(Collections.singletonList(new ScanRange(null, fromKey, null, toKey)),
            (prefix, key, value, timestamp, version) -> {
                count[0]++;
                return true;
            },
            false);
        return count[0];
    }

    public TakeCursors take(EmbeddedClient client, Map<String, NamedCursor> cursorsByName, TxKeyValueStream scan) throws Exception {
        String localRingMemberName = amzaService.getRingReader().getRingMember().getMember();
        NamedCursor localNamedCursor = cursorsByName.get(localRingMemberName);
        long transactionId = (localNamedCursor != null) ? localNamedCursor.id : 0;

        return client.takeFromTransactionId(transactionId, scan);
    }

    public TakeCursors take(EmbeddedClient client, Map<String, NamedCursor> cursorsByName, byte[] prefix, TxKeyValueStream scan) throws Exception {
        String localRingMemberName = amzaService.getRingReader().getRingMember().getMember();
        NamedCursor localNamedCursor = cursorsByName.get(localRingMemberName);
        long transactionId = (localNamedCursor != null) ? localNamedCursor.id : 0;

        return client.takeFromTransactionId(prefix, transactionId, scan);
    }

    public AmzaCursor scan(EmbeddedClient client,
        Map<String, NamedCursor> cursorsByName,
        byte[] prefix,
        KeyValueTimestampStream scan) throws Exception {

        RingMember localRingMember = amzaService.getRingReader().getRingMember();
        String localRingMemberName = localRingMember.getMember();
        NamedCursor localNamedCursor = cursorsByName.get(localRingMemberName);
        long id = (localNamedCursor != null) ? localNamedCursor.id : 0;

        long[] nextId = new long[1];
        client.scan(
            Collections.singletonList(new ScanRange(prefix, FilerIO.longBytes(id), prefix, FilerIO.longBytes(Long.MAX_VALUE))),
            (prefix1, key, value, timestamp, version) -> {
                nextId[0] = FilerIO.bytesLong(key);
                return scan.stream(prefix1, key, value, timestamp, version);
            },
            true
        );
        cursorsByName.put(localRingMemberName, new NamedCursor(localRingMemberName, nextId[0]));
        return new AmzaCursor(cursorsByName.values(), null);
    }

    public byte[] toPartitionsKey(MiruTenantId tenantId, MiruPartitionId partitionId) {
        if (partitionId != null) {
            byte[] tenantBytes = tenantId.getBytes();
            ByteBuffer buf = ByteBuffer.allocate(4 + tenantBytes.length + 4);
            buf.putInt(tenantBytes.length);
            buf.put(tenantBytes);
            buf.putInt(partitionId.getId());
            return buf.array();
        } else {
            byte[] tenantBytes = tenantId.getBytes();
            ByteBuffer buf = ByteBuffer.allocate(4 + tenantBytes.length);
            buf.putInt(tenantBytes.length);
            buf.put(tenantBytes);
            return buf.array();
        }
    }

    public TenantAndPartition fromPartitionsKey(byte[] key) {
        ByteBuffer buf = ByteBuffer.wrap(key);
        int tenantLength = buf.getInt();
        byte[] tenantBytes = new byte[tenantLength];
        buf.get(tenantBytes);
        int partitionId = buf.getInt();
        return new TenantAndPartition(new MiruTenantId(tenantBytes), MiruPartitionId.of(partitionId));
    }

    public Map<String, NamedCursor> extractCursors(List<NamedCursor> cursors) {
        Map<String, NamedCursor> cursorsByName = Maps.newHashMapWithExpectedSize(cursors.size());
        for (NamedCursor namedCursor : cursors) {
            cursorsByName.put(namedCursor.name, namedCursor);
        }
        return cursorsByName;
    }

    public void mergeCursors(Map<String, NamedCursor> cursorsByName, TakeCursors takeCursors) {
        for (TakeCursors.RingMemberCursor memberCursor : takeCursors.ringMemberCursors) {
            String memberName = memberCursor.ringMember.getMember();
            NamedCursor existing = cursorsByName.get(memberName);
            if (existing == null || memberCursor.transactionId > existing.id) {
                cursorsByName.put(memberName, new NamedCursor(memberName, memberCursor.transactionId));
            }
        }
    }

    public String getRingMemberName() {
        return amzaService.getRingReader().getRingMember().getMember();
    }
}
