package com.jivesoftware.os.miru.wal.lookup;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.service.AmzaRegion;
import com.jivesoftware.os.amza.shared.RegionName;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALValue;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityLookupEntry;
import com.jivesoftware.os.miru.wal.AmzaWALUtil;
import com.jivesoftware.os.rcvs.marshall.api.UtilLexMarshaller;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class AmzaWALLookup implements MiruWALLookup {

    private final AmzaWALUtil amzaWALUtil;
    private final int ringSize;
    private final MiruActivityLookupEntryMarshaller activityLookupEntryMarshaller = new MiruActivityLookupEntryMarshaller();
    private final Set<MiruTenantId> knownLookupTenants = Collections.newSetFromMap(Maps.newConcurrentMap());

    public AmzaWALLookup(AmzaWALUtil amzaWALUtil,
        int ringSize) {
        this.amzaWALUtil = amzaWALUtil;
        this.ringSize = ringSize;
    }

    @Override
    public MiruVersionedActivityLookupEntry[] getVersionedEntries(MiruTenantId tenantId, final Long[] activityTimestamps) throws Exception {
        String lookupName = "lookup-" + tenantId.toString();
        RegionName regionName = new RegionName(false, lookupName, lookupName);
        AmzaRegion region = amzaWALUtil.getRegion(regionName);

        MiruVersionedActivityLookupEntry[] entries = new MiruVersionedActivityLookupEntry[activityTimestamps.length];
        for (int i = 0; i < activityTimestamps.length; i++) {
            WALKey key = new WALKey(FilerIO.longBytes(activityTimestamps[i]));
            WALValue value = region.getValue(key);
            if (value != null && !value.getTombstoned()) {
                entries[i] = new MiruVersionedActivityLookupEntry(value.getTimestampId(), activityLookupEntryMarshaller.fromBytes(value.getValue()));
            }
        }
        return entries;
    }

    @Override
    public void add(MiruTenantId tenantId, List<MiruPartitionedActivity> activities) throws Exception {
        if (!knownLookupTenants.contains(tenantId)) {
            AmzaRegion lookupTenantsRegion = amzaWALUtil.getOrCreateMaximalRegion(AmzaWALUtil.LOOKUP_TENANTS_REGION_NAME, Optional.<RegionProperties>absent());
            lookupTenantsRegion.setValue(new WALKey(tenantId.getBytes()), AmzaWALUtil.NULL_VALUE);
            knownLookupTenants.add(tenantId);
        }

        String lookupName = "lookup-activity-" + tenantId.toString();
        RegionName regionName = new RegionName(false, lookupName, lookupName);
        AmzaRegion lookupActivityRegion = amzaWALUtil.getOrCreateRegion(regionName, ringSize, Optional.<RegionProperties>absent());

        Map<MiruPartitionId, RangeMinMax> partitionMinMax = Maps.newHashMap();
        for (MiruPartitionedActivity activity : activities) {
            if (activity.type.isActivityType()) {
                RangeMinMax rangeMinMax = partitionMinMax.get(activity.partitionId);
                if (rangeMinMax == null) {
                    rangeMinMax = new RangeMinMax();
                    partitionMinMax.put(activity.partitionId, rangeMinMax);
                }
                rangeMinMax.put(activity.clockTimestamp, activity.timestamp);

                boolean removed = MiruPartitionedActivity.Type.REMOVE.equals(activity.type);
                MiruActivityLookupEntry entry = new MiruActivityLookupEntry(activity.partitionId.getId(), activity.index, activity.writerId, removed);
                long version = activity.activity.get().version;
                lookupActivityRegion.setValue(new WALKey(FilerIO.longBytes(activity.timestamp)),
                    new WALValue(activityLookupEntryMarshaller.toBytes(entry), version, false));
            }
        }

        for (Map.Entry<MiruPartitionId, RangeMinMax> entry : partitionMinMax.entrySet()) {
            putRange(tenantId, entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void stream(MiruTenantId tenantId, long afterTimestamp, final StreamLookupEntry streamLookupEntry) throws Exception {
        String lookupName = "lookup-activity-" + tenantId.toString();
        RegionName regionName = new RegionName(false, lookupName, lookupName);
        AmzaRegion region = amzaWALUtil.getRegion(regionName);

        if (region != null) {
            region.rangeScan(new WALKey(FilerIO.longBytes(afterTimestamp)), null, (rowTxId, key, value) -> {
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
        AmzaRegion region = amzaWALUtil.getRegion(AmzaWALUtil.LOOKUP_TENANTS_REGION_NAME);

        final List<MiruTenantId> tenantIds = Lists.newArrayList();
        if (region != null) {
            region.scan((rowTxId, key, value) -> {
                if (key != null) {
                    tenantIds.add(new MiruTenantId(key.getKey()));
                }
                return true;
            });
        }
        return tenantIds;
    }

    private WALKey tenantPartitionToKeyBytes(MiruTenantId tenantId, MiruPartitionId partitionId, RangeType rangeType) {
        byte[] keyBytes;
        byte[] tenantBytes = tenantId.getBytes();
        if (partitionId != null) {
            if (rangeType != null) {
                keyBytes = new byte[4 + tenantBytes.length + 4 + 1];
                FilerIO.intBytes(tenantBytes.length, keyBytes, 0);
                System.arraycopy(tenantBytes, 0, keyBytes, 4, tenantBytes.length);
                UtilLexMarshaller.intBytes(partitionId.getId(), keyBytes, 4 + tenantBytes.length);
                keyBytes[keyBytes.length - 1] = rangeType.getType();
            } else {
                keyBytes = new byte[4 + tenantBytes.length + 4];
                FilerIO.intBytes(tenantBytes.length, keyBytes, 0);
                System.arraycopy(tenantBytes, 0, keyBytes, 4, tenantBytes.length);
                UtilLexMarshaller.intBytes(partitionId.getId(), keyBytes, 4 + tenantBytes.length);
            }
        } else {
            keyBytes = new byte[4 + tenantBytes.length];
            FilerIO.intBytes(tenantBytes.length, keyBytes, 0);
            System.arraycopy(tenantBytes, 0, keyBytes, 4, tenantBytes.length);
        }
        return new WALKey(keyBytes);
    }

    private MiruPartitionId keyBytesToPartitionId(byte[] keyBytes) {
        return MiruPartitionId.of(FilerIO.bytesInt(keyBytes, keyBytes.length - 4 - 1));
    }

    private RangeType keyBytesToRangeType(byte[] keyBytes) {
        return RangeType.fromType(keyBytes[keyBytes.length - 1]);
    }

    @Override
    public void streamRanges(MiruTenantId tenantId, MiruPartitionId partitionId, StreamRangeLookup streamRangeLookup) throws Exception {
        WALKey fromKey = tenantPartitionToKeyBytes(tenantId, partitionId, null);
        WALKey toKey = fromKey.prefixUpperExclusive();
        AmzaRegion region = amzaWALUtil.getRegion(AmzaWALUtil.LOOKUP_RANGES_REGION_NAME);
        if (region != null) {
            region.rangeScan(fromKey, toKey, (rowTxId, key, value) -> {
                if (value != null) {
                    MiruPartitionId streamPartitionId = keyBytesToPartitionId(key.getKey());
                    if (partitionId == null || partitionId.equals(streamPartitionId)) {
                        RangeType rangeType = keyBytesToRangeType(key.getKey());
                        if (streamRangeLookup.stream(streamPartitionId, rangeType, value.getTimestampId())) {
                            return true;
                        }
                    }
                }
                return false;
            });
        }
    }

    @Override
    public void putRange(MiruTenantId tenantId, MiruPartitionId partitionId, RangeMinMax rangeMinMax) throws Exception {
        AmzaRegion region = amzaWALUtil.getOrCreateMaximalRegion(AmzaWALUtil.LOOKUP_RANGES_REGION_NAME, Optional.<RegionProperties>absent());
        region.setValues(Arrays.asList(
            new SimpleEntry<>(tenantPartitionToKeyBytes(tenantId, partitionId, RangeType.clockMin),
                new WALValue(null, Long.MAX_VALUE - rangeMinMax.getMinClock(), false)),
            new SimpleEntry<>(tenantPartitionToKeyBytes(tenantId, partitionId, RangeType.clockMax),
                new WALValue(null, rangeMinMax.getMaxClock(), false)),
            new SimpleEntry<>(tenantPartitionToKeyBytes(tenantId, partitionId, RangeType.orderIdMin),
                new WALValue(null, Long.MAX_VALUE - rangeMinMax.getMinOrderId(), false)),
            new SimpleEntry<>(tenantPartitionToKeyBytes(tenantId, partitionId, RangeType.orderIdMax),
                new WALValue(null, rangeMinMax.getMaxOrderId(), false))));
    }
}
