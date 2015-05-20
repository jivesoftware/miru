package com.jivesoftware.os.miru.wal.lookup;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.MiruVoidByte;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.api.wal.MiruActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruVersionedActivityLookupEntry;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.timestamper.ConstantTimestamper;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class RCVSWALLookup implements MiruWALLookup {

    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> activityLookupTable;

    public RCVSWALLookup(RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> activityLookupTable) {
        this.activityLookupTable = activityLookupTable;
    }

    @Override
    public MiruVersionedActivityLookupEntry[] getVersionedEntries(MiruTenantId tenantId, final Long[] activityTimestamps) throws Exception {

        ColumnValueAndTimestamp<Long, MiruActivityLookupEntry, Long>[] got = activityLookupTable.multiGetEntries(MiruVoidByte.INSTANCE,
            tenantId, activityTimestamps, null, null);

        MiruVersionedActivityLookupEntry[] entrys = new MiruVersionedActivityLookupEntry[activityTimestamps.length];
        for (int i = 0; i < entrys.length; i++) {
            if (got[i] == null) {
                entrys[i] = null;
            } else {
                entrys[i] = new MiruVersionedActivityLookupEntry(got[i].getTimestamp(), got[i].getValue());
            }
        }
        return entrys;
    }

    @Override
    public Map<MiruPartitionId, RangeMinMax> add(MiruTenantId tenantId, List<MiruPartitionedActivity> activities) throws Exception {
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
                activityLookupTable.add(MiruVoidByte.INSTANCE, tenantId, activity.timestamp, entry, null, new ConstantTimestamper(version));
            }
        }

        return partitionMinMax;
    }

    @Override
    public void stream(MiruTenantId tenantId, long afterTimestamp, final StreamLookupEntry streamLookupEntry) throws Exception {

        activityLookupTable.getEntrys(MiruVoidByte.INSTANCE, tenantId, afterTimestamp, Long.MAX_VALUE, 1_000, false, null, null,
            new CallbackStream<ColumnValueAndTimestamp<Long, MiruActivityLookupEntry, Long>>() {
                @Override
                public ColumnValueAndTimestamp<Long, MiruActivityLookupEntry, Long> callback(
                    ColumnValueAndTimestamp<Long, MiruActivityLookupEntry, Long> v) throws Exception {

                    if (v != null) {
                        if (!streamLookupEntry.stream(v.getColumn(), v.getValue(), v.getTimestamp())) {
                            return null;
                        }
                    }
                    return v;
                }
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
