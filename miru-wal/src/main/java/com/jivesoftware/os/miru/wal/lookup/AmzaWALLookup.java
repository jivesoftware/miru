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
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.api.wal.MiruActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.miru.wal.AmzaWALUtil;
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
    public Map<MiruPartitionId, RangeMinMax> add(MiruTenantId tenantId, List<MiruPartitionedActivity> activities) throws Exception {
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
            RangeMinMax rangeMinMax = partitionMinMax.get(activity.partitionId);
            if (rangeMinMax == null) {
                rangeMinMax = new RangeMinMax();
                partitionMinMax.put(activity.partitionId, rangeMinMax);
            }
            if (activity.type.isActivityType()) {
                rangeMinMax.put(activity.clockTimestamp, activity.timestamp);

                boolean removed = MiruPartitionedActivity.Type.REMOVE.equals(activity.type);
                MiruActivityLookupEntry entry = new MiruActivityLookupEntry(activity.partitionId.getId(), activity.index, activity.writerId, removed);
                long version = activity.activity.get().version;
                lookupActivityRegion.setValue(new WALKey(FilerIO.longBytes(activity.timestamp)),
                    new WALValue(activityLookupEntryMarshaller.toBytes(entry), version, false));
            }
        }

        return partitionMinMax;
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

}
