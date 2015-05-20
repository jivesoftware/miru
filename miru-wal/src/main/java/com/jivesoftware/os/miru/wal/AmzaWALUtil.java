package com.jivesoftware.os.miru.wal;

import com.google.common.base.Optional;
import com.jivesoftware.os.amza.service.AmzaRegion;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.MemoryWALUpdates;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.Scan;
import com.jivesoftware.os.amza.shared.TakeCursors;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 *
 */
public class AmzaWALUtil {

    public static final RegionName LOOKUP_TENANTS_REGION_NAME = new RegionName(false, "lookup-tenants", "lookup-tenants");
    public static final RegionName LOOKUP_PARTITIONS_REGION_NAME = new RegionName(false, "lookup-partitions", "lookup-partitions");

    public static final WALValue NULL_VALUE = new WALValue(null, 0L, false);

    private final AmzaService amzaService;
    private final RegionProperties defaultProperties;

    public AmzaWALUtil(AmzaService amzaService, RegionProperties defaultProperties) {
        this.amzaService = amzaService;
        this.defaultProperties = defaultProperties;
    }

    public AmzaRegion getOrCreateRegion(RegionName regionName, int ringSize, Optional<RegionProperties> regionProperties) throws Exception {
        amzaService.getAmzaRing().ensureSubRing(regionName.getRingName(), ringSize);
        return amzaService.createRegionIfAbsent(regionName, regionProperties.or(defaultProperties));
    }

    public AmzaRegion getOrCreateMaximalRegion(RegionName regionName, Optional<RegionProperties> regionProperties) throws Exception {
        amzaService.getAmzaRing().ensureMaximalSubRing(regionName.getRingName());
        return amzaService.createRegionIfAbsent(regionName, regionProperties.or(defaultProperties));
    }

    public AmzaRegion getRegion(RegionName regionName) throws Exception {
        return amzaService.getRegion(regionName);
    }

    public boolean hasRegion(RegionName regionName) throws Exception {
        return amzaService.hasRegion(regionName);
    }

    public boolean replicate(RegionName regionName, MemoryWALUpdates updates, int requireNReplicas) throws Exception {
        return amzaService.replicate(regionName, updates, requireNReplicas);
    }

    public TakeCursors take(AmzaRegion region, Map<String, NamedCursor> cursorsByName, Scan<WALValue> scan) throws Exception {
        String localRingHostName = amzaService.getAmzaRing().getRingHost().toCanonicalString();
        NamedCursor localNamedCursor = cursorsByName.get(localRingHostName);
        long transactionId = (localNamedCursor != null) ? localNamedCursor.id : 0;

        return amzaService.takeFromTransactionId(region, transactionId, scan);
    }

    public WALKey toPartitionsKey(MiruTenantId tenantId, MiruPartitionId partitionId) {
        byte[] tenantBytes = tenantId.getBytes();
        ByteBuffer buf = ByteBuffer.allocate(4 + tenantBytes.length + 4);
        buf.putInt(tenantBytes.length);
        buf.put(tenantBytes);
        buf.putInt(partitionId.getId());
        return new WALKey(buf.array());
    }

    public TenantAndPartition fromPartitionsKey(WALKey key) {
        ByteBuffer buf = ByteBuffer.wrap(key.getKey());
        int tenantLength = buf.getInt();
        byte[] tenantBytes = new byte[tenantLength];
        buf.get(tenantBytes);
        int partitionId = buf.getInt();
        return new TenantAndPartition(new MiruTenantId(tenantBytes), MiruPartitionId.of(partitionId));
    }

}
