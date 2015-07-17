package com.jivesoftware.os.miru.wal;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.jivesoftware.os.amza.client.AmzaKretrProvider;
import com.jivesoftware.os.amza.client.AmzaKretrProvider.AmzaKretr;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.TimestampedValue;
import com.jivesoftware.os.amza.shared.partition.PartitionName;
import com.jivesoftware.os.amza.shared.partition.PartitionProperties;
import com.jivesoftware.os.amza.shared.scan.Scan;
import com.jivesoftware.os.amza.shared.take.TakeCursors;
import com.jivesoftware.os.amza.shared.wal.WALValue;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
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
    private static final PartitionName LOOKUP_PARTITIONS_REGION_NAME = new PartitionName(false,
        "lookup-partitions".getBytes(Charsets.UTF_8),
        "lookup-partitions".getBytes(Charsets.UTF_8));

    public static final WALValue NULL_VALUE = new WALValue(null, 0L, false);

    private final AmzaService amzaService;
    private final AmzaKretrProvider amzaKretrProvider;
    private final PartitionProperties defaultProperties;

    public AmzaWALUtil(AmzaService amzaService,
        AmzaKretrProvider amzaKretrProvider,
        PartitionProperties defaultProperties) {
        this.amzaService = amzaService;
        this.amzaKretrProvider = amzaKretrProvider;
        this.defaultProperties = defaultProperties;
    }

    public HostPort[] getActivityRoutingGroup(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        Optional<PartitionProperties> regionProperties) throws Exception {
        PartitionName partitionName = getActivityPartitionName(tenantId, partitionId);
        amzaService.getRingWriter().ensureMaximalSubRing(partitionName.getRingName());
        amzaService.setPropertiesIfAbsent(partitionName, regionProperties.or(defaultProperties));
        return amzaService.getPartitionRoute(partitionName).orderedPartitionHosts.stream()
            .map(ringHost -> new HostPort(ringHost.getHost(), ringHost.getPort()))
            .toArray(HostPort[]::new);
    }

    public HostPort[] getLookupRoutingGroup(MiruTenantId tenantId) throws Exception {
        return amzaService.getPartitionRoute(getLookupPartitionName(tenantId)).orderedPartitionHosts.stream()
            .map(ringHost -> new HostPort(ringHost.getHost(), ringHost.getPort()))
            .toArray(HostPort[]::new);
    }

    private PartitionName getActivityPartitionName(MiruTenantId tenantId, MiruPartitionId partitionId) {
        String walName = "activityWAL-" + tenantId.toString() + "-" + partitionId.toString();
        byte[] walNameBytes = walName.getBytes(Charsets.UTF_8);
        return new PartitionName(false, walNameBytes, walNameBytes);
    }

    private PartitionName getLookupPartitionName(MiruTenantId tenantId) {
        String lookupName = "lookup-activity-" + tenantId.toString();
        byte[] lookupNameBytes = lookupName.getBytes(Charsets.UTF_8);
        return new PartitionName(false, lookupNameBytes, lookupNameBytes);
    }

    /* TODO should be done by the remote client looking up ordered hosts
    public AmzaKretr getOrCreateClient(PartitionName partitionName, int ringSize, Optional<PartitionProperties> regionProperties) throws Exception {
        amzaService.getAmzaHostRing().ensureSubRing(partitionName.getRingName(), ringSize);
        amzaService.setPropertiesIfAbsent(partitionName, regionProperties.or(defaultProperties));
        return amzaKretrProvider.getClient(partitionName);
    }
    */

    private AmzaKretr getClient(PartitionName partitionName) throws Exception {
        return amzaKretrProvider.getClient(partitionName);
    }

    public AmzaKretr getActivityClient(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return getClient(getActivityPartitionName(tenantId, partitionId));
    }

    public AmzaKretr getLookupClient(MiruTenantId tenantId) throws Exception {
        return getClient(getLookupPartitionName(tenantId));
    }

    public AmzaKretr getLookupTenantsClient() throws Exception {
        return getOrCreateMaximalClient(LOOKUP_TENANTS_PARTITION_NAME, Optional.<PartitionProperties>absent());
    }

    public AmzaKretr getLookupPartitionsClient() throws Exception {
        return getOrCreateMaximalClient(LOOKUP_TENANTS_PARTITION_NAME, Optional.<PartitionProperties>absent());
    }

    private AmzaKretr getOrCreateMaximalClient(PartitionName partitionName, Optional<PartitionProperties> regionProperties) throws Exception {
        amzaService.getRingWriter().ensureMaximalSubRing(partitionName.getRingName());
        amzaService.setPropertiesIfAbsent(partitionName, regionProperties.or(defaultProperties));
        return amzaKretrProvider.getClient(partitionName);
    }

    public boolean hasActivityPartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return amzaService.hasPartition(getActivityPartitionName(tenantId, partitionId));
    }

    public void destroyActivityPartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        amzaService.destroyPartition(getActivityPartitionName(tenantId, partitionId));
    }

    public TakeCursors take(AmzaKretr client, Map<String, NamedCursor> cursorsByName, Scan<TimestampedValue> scan) throws Exception {
        String localRingMemberName = amzaService.getRingReader().getRingMember().getMember();
        NamedCursor localNamedCursor = cursorsByName.get(localRingMemberName);
        long transactionId = (localNamedCursor != null) ? localNamedCursor.id : 0;

        return client.takeFromTransactionId(transactionId, scan);
    }

    public byte[] toPartitionsKey(MiruTenantId tenantId, MiruPartitionId partitionId) {
        byte[] tenantBytes = tenantId.getBytes();
        ByteBuffer buf = ByteBuffer.allocate(4 + tenantBytes.length + 4);
        buf.putInt(tenantBytes.length);
        buf.put(tenantBytes);
        buf.putInt(partitionId.getId());
        return buf.array();
    }

    public TenantAndPartition fromPartitionsKey(byte[] key) {
        ByteBuffer buf = ByteBuffer.wrap(key);
        int tenantLength = buf.getInt();
        byte[] tenantBytes = new byte[tenantLength];
        buf.get(tenantBytes);
        int partitionId = buf.getInt();
        return new TenantAndPartition(new MiruTenantId(tenantBytes), MiruPartitionId.of(partitionId));
    }

}
