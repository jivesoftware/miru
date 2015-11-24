package com.jivesoftware.os.miru.wal;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.client.AmzaClientProvider;
import com.jivesoftware.os.amza.client.AmzaClientProvider.AmzaClient;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.partition.VersionedPartitionName;
import com.jivesoftware.os.amza.shared.ring.RingMember;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.take.TakeCursors;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import com.jivesoftware.os.miru.api.wal.AmzaCursor;
import com.jivesoftware.os.miru.wal.lookup.PartitionsStream;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.nio.ByteBuffer;
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
    private final AmzaClientProvider amzaClientProvider;
    private final PartitionProperties defaultProperties;
    private final Map<PartitionName, AmzaClient> clientMap = Maps.newConcurrentMap();

    public AmzaWALUtil(AmzaService amzaService,
        AmzaClientProvider amzaClientProvider,
        PartitionProperties defaultProperties) {
        this.amzaService = amzaService;
        this.amzaClientProvider = amzaClientProvider;
        this.defaultProperties = defaultProperties;
    }

    public HostPort[] getActivityRoutingGroup(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        Optional<PartitionProperties> regionProperties) throws Exception {
        PartitionName partitionName = getActivityPartitionName(tenantId, partitionId);
        amzaService.getRingWriter().ensureSubRing(partitionName.getRingName(), 3); //TODO config numberOfReplicas
        amzaService.setPropertiesIfAbsent(partitionName, regionProperties.or(defaultProperties));
        return amzaService.getPartitionRoute(partitionName).orderedPartitionHosts.stream()
            .map(ringHost -> new HostPort(ringHost.getHost(), ringHost.getPort()))
            .toArray(HostPort[]::new);
    }

    public HostPort[] getReadTrackingRoutingGroup(MiruTenantId tenantId, Optional<PartitionProperties> partitionProperties) throws Exception {
        PartitionName partitionName = getReadTrackingPartitionName(tenantId);
        amzaService.getRingWriter().ensureSubRing(partitionName.getRingName(), 3); //TODO config numberOfReplicas
        amzaService.setPropertiesIfAbsent(partitionName, partitionProperties.or(defaultProperties));
        return amzaService.getPartitionRoute(partitionName).orderedPartitionHosts.stream()
            .map(ringHost -> new HostPort(ringHost.getHost(), ringHost.getPort()))
            .toArray(HostPort[]::new);
    }

    //TODO slit my wrists
    public void allActivityPartitions(PartitionsStream partitionsStream) throws Exception {
        byte[] prefix = "activityWAL-".getBytes(Charsets.UTF_8);

        partition:
        for (VersionedPartitionName versionedPartitionName : amzaService.getPartitionIndex().getAllPartitions()) {
            byte[] nameBytes = versionedPartitionName.getPartitionName().getName();
            if (nameBytes.length > prefix.length) {
                for (int i = 0; i < prefix.length; i++) {
                    if (nameBytes[i] != prefix[i]) {
                        continue partition;
                    }
                }
                String partitionName = new String(nameBytes, Charsets.UTF_8);
                int firstHyphen = partitionName.indexOf('-');
                int lastHyphen = partitionName.lastIndexOf('-');
                if (!partitionsStream.stream(new MiruTenantId(partitionName.substring(firstHyphen + 1, lastHyphen).getBytes(Charsets.UTF_8)),
                    MiruPartitionId.of(Integer.parseInt(partitionName.substring(lastHyphen + 1))))) {
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

    private PartitionName getLookupPartitionName(MiruTenantId tenantId) {
        String lookupName = "lookup-activity-" + tenantId.toString();
        byte[] lookupNameBytes = lookupName.getBytes(Charsets.UTF_8);
        return new PartitionName(false, lookupNameBytes, lookupNameBytes);
    }

    /* TODO should be done by the remote client looking up ordered hosts
    public AmzaClient getOrCreateClient(PartitionName partitionName, int ringSize, Optional<PartitionProperties> regionProperties) throws Exception {
        amzaService.getAmzaHostRing().ensureSubRing(partitionName.getRingName(), ringSize);
        amzaService.setPropertiesIfAbsent(partitionName, regionProperties.or(defaultProperties));
        return amzaClientProvider.getClient(partitionName);
    }
    */

    private AmzaClient getClient(PartitionName partitionName) throws Exception {
        return amzaClientProvider.getClient(partitionName);
    }

    public AmzaClient getActivityClient(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return getClient(getActivityPartitionName(tenantId, partitionId));
    }

    public AmzaClient getReadTrackingClient(MiruTenantId tenantId) throws Exception {
        return getClient(getReadTrackingPartitionName(tenantId));
    }

    public AmzaClient getLookupClient(MiruTenantId tenantId) throws Exception {
        return getClient(getLookupPartitionName(tenantId));
    }

    public AmzaClient getLookupTenantsClient() throws Exception {
        return getOrCreateMaximalClient(LOOKUP_TENANTS_PARTITION_NAME, Optional.<PartitionProperties>absent());
    }

    public AmzaClient getLookupPartitionsClient() throws Exception {
        return getOrCreateMaximalClient(LOOKUP_PARTITIONS_PARTITION_NAME, Optional.<PartitionProperties>absent());
    }

    private AmzaClient getOrCreateMaximalClient(PartitionName partitionName, Optional<PartitionProperties> partitionProperties) throws Exception {
        return clientMap.computeIfAbsent(partitionName, key -> {
            try {
                amzaService.getRingWriter().ensureMaximalSubRing(partitionName.getRingName());
                amzaService.setPropertiesIfAbsent(partitionName, partitionProperties.or(defaultProperties));
                amzaService.awaitOnline(partitionName, 10_000); //TODO config
                return amzaClientProvider.getClient(partitionName);
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

    public TakeCursors take(AmzaClient client, Map<String, NamedCursor> cursorsByName, Scan<TimestampedValue> scan) throws Exception {
        String localRingMemberName = amzaService.getRingReader().getRingMember().getMember();
        NamedCursor localNamedCursor = cursorsByName.get(localRingMemberName);
        long transactionId = (localNamedCursor != null) ? localNamedCursor.id : 0;

        return client.takeFromTransactionId(transactionId, scan);
    }

    public TakeCursors take(AmzaClient client, Map<String, NamedCursor> cursorsByName, byte[] prefix, Scan<TimestampedValue> scan) throws Exception {
        String localRingMemberName = amzaService.getRingReader().getRingMember().getMember();
        NamedCursor localNamedCursor = cursorsByName.get(localRingMemberName);
        long transactionId = (localNamedCursor != null) ? localNamedCursor.id : 0;

        return client.takeFromTransactionId(prefix, transactionId, scan);
    }

    public AmzaCursor scan(AmzaClient client,
        Map<String, NamedCursor> cursorsByName,
        byte[] prefix,
        Scan<TimestampedValue> scan) throws Exception {

        RingMember localRingMember = amzaService.getRingReader().getRingMember();
        String localRingMemberName = localRingMember.getMember();
        NamedCursor localNamedCursor = cursorsByName.get(localRingMemberName);
        long id = (localNamedCursor != null) ? localNamedCursor.id : 0;

        long[] nextId = new long[1];
        client.scan(prefix, FilerIO.longBytes(id), prefix, FilerIO.longBytes(Long.MAX_VALUE), (rowTxId, _prefix, key, scanned) -> {
            nextId[0] = FilerIO.bytesLong(key);
            return scan.row(rowTxId, prefix, key, scanned);
        });
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
