package com.jivesoftware.os.miru.wal.lookup;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.MiruVoidByte;
import com.jivesoftware.os.miru.api.wal.MiruActivityLookupEntry;
import com.jivesoftware.os.miru.api.wal.MiruRangeLookupColumnKey;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.RowColumValueTimestampAdd;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.timestamper.ConstantTimestamper;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MiruRCVSWALLookup implements MiruWALLookup {

    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> activityLookupTable;
    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruRangeLookupColumnKey, Long, ? extends Exception> rangeLookupTable;

    public MiruRCVSWALLookup(RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> activityLookupTable,
        RowColumnValueStore<MiruVoidByte, MiruTenantId, MiruRangeLookupColumnKey, Long, ? extends Exception> rangeLookupTable) {
        this.activityLookupTable = activityLookupTable;
        this.rangeLookupTable = rangeLookupTable;
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
    public void add(MiruTenantId tenantId, List<MiruPartitionedActivity> activities) throws Exception {
        Map<MiruPartitionId, RangeMinMax> partitionMinMax = Maps.newHashMap();
        for (MiruPartitionedActivity activity : activities) {
            RangeMinMax rangeMinMax = partitionMinMax.get(activity.partitionId);
            if (rangeMinMax == null) {
                rangeMinMax = new RangeMinMax();
                partitionMinMax.put(activity.partitionId, rangeMinMax);
            }
            rangeMinMax.put(activity.clockTimestamp, activity.timestamp);

            if (activity.type.isActivityType()) {
                boolean removed = MiruPartitionedActivity.Type.REMOVE.equals(activity.type);
                MiruActivityLookupEntry entry = new MiruActivityLookupEntry(activity.partitionId.getId(), activity.index, activity.writerId, removed);
                long version = activity.activity.get().version;
                activityLookupTable.add(MiruVoidByte.INSTANCE, tenantId, activity.timestamp, entry, null, new ConstantTimestamper(version));
            }
        }

        for (Map.Entry<MiruPartitionId, RangeMinMax> entry : partitionMinMax.entrySet()) {
            putRange(tenantId, entry.getKey(), entry.getValue());
        }
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

    @Override
    public void streamRanges(MiruTenantId tenantId, final MiruPartitionId partitionId, final StreamRangeLookup streamRangeLookup) throws Exception {
        MiruRangeLookupColumnKey rangeLookupColumnKey = null;
        long maxCount = Long.MAX_VALUE;
        int batchSize = 1_000;
        if (partitionId != null) {
            rangeLookupColumnKey = new MiruRangeLookupColumnKey(partitionId.getId(), (byte) 0);
            maxCount = RangeType.values().length;
            batchSize = RangeType.values().length + 1;
        }
        rangeLookupTable.getEntrys(MiruVoidByte.INSTANCE, tenantId, rangeLookupColumnKey, maxCount, batchSize, false, null, null,
            new CallbackStream<ColumnValueAndTimestamp<MiruRangeLookupColumnKey, Long, Long>>() {
                @Override
                public ColumnValueAndTimestamp<MiruRangeLookupColumnKey, Long, Long> callback(
                    ColumnValueAndTimestamp<MiruRangeLookupColumnKey, Long, Long> v) throws Exception {

                    if (v != null) {
                        MiruPartitionId streamPartitionId = MiruPartitionId.of(v.getColumn().partitionId);
                        if (partitionId == null || partitionId.equals(streamPartitionId)) {
                            if (streamRangeLookup.stream(streamPartitionId, RangeType.fromType(v.getColumn().type), v.getValue())) {
                                return v;
                            }
                        }
                    }
                    return null;
                }
            });
    }

    @Override
    public void putRange(MiruTenantId tenantId, MiruPartitionId partitionId, RangeMinMax rangeMinMax) throws Exception {
        rangeLookupTable.multiRowsMultiAdd(MiruVoidByte.INSTANCE, Arrays.<RowColumValueTimestampAdd<MiruTenantId, MiruRangeLookupColumnKey, Long>>asList(
            new RowColumValueTimestampAdd<>(tenantId, new MiruRangeLookupColumnKey(partitionId.getId(), RangeType.clockMin.getType()),
                rangeMinMax.getMinClock(), new ConstantTimestamper(Long.MAX_VALUE - rangeMinMax.getMinClock())),
            new RowColumValueTimestampAdd<>(tenantId, new MiruRangeLookupColumnKey(partitionId.getId(), RangeType.clockMax.getType()),
                rangeMinMax.getMaxClock(), new ConstantTimestamper(rangeMinMax.getMaxClock())),
            new RowColumValueTimestampAdd<>(tenantId, new MiruRangeLookupColumnKey(partitionId.getId(), RangeType.orderIdMin.getType()),
                rangeMinMax.getMinOrderId(), new ConstantTimestamper(Long.MAX_VALUE - rangeMinMax.getMinOrderId())),
            new RowColumValueTimestampAdd<>(tenantId, new MiruRangeLookupColumnKey(partitionId.getId(), RangeType.orderIdMax.getType()),
                rangeMinMax.getMaxOrderId(), new ConstantTimestamper(rangeMinMax.getMaxOrderId()))));
    }
}
