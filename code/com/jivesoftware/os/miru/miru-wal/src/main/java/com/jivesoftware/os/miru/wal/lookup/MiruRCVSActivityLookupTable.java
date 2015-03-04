package com.jivesoftware.os.miru.wal.lookup;

import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.MiruVoidByte;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.TenantIdAndRow;
import com.jivesoftware.os.rcvs.api.timestamper.ConstantTimestamper;
import java.util.List;

/**
 *
 */
public class MiruRCVSActivityLookupTable implements MiruActivityLookupTable {

    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> lookupTable;

    public MiruRCVSActivityLookupTable(RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> lookupTable) {
        this.lookupTable = lookupTable;
    }

    @Override
    public MiruVersionedActivityLookupEntry[] getVersionedEntries(MiruTenantId tenantId, final Long[] activityTimestamps) throws Exception {

        ColumnValueAndTimestamp<Long, MiruActivityLookupEntry, Long>[] got = lookupTable.multiGetEntries(MiruVoidByte.INSTANCE,
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
        for (MiruPartitionedActivity activity : activities) {
            if (activity.type.isActivityType()) {
                boolean removed = MiruPartitionedActivity.Type.REMOVE.equals(activity.type);
                MiruActivityLookupEntry entry = new MiruActivityLookupEntry(activity.partitionId.getId(), activity.index, activity.writerId, removed);
                long version = activity.activity.get().version;
                lookupTable.add(MiruVoidByte.INSTANCE, tenantId, activity.timestamp, entry, null, new ConstantTimestamper(version));
            }
        }
    }

    @Override
    public void stream(MiruTenantId tenantId, long afterTimestamp, final StreamLookupEntry streamLookupEntry) throws Exception {

        lookupTable.getEntrys(MiruVoidByte.INSTANCE, tenantId, afterTimestamp, Long.MAX_VALUE, 1_000, false, null, null,
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
        lookupTable.getAllRowKeys(10_000, null, new CallbackStream<TenantIdAndRow<MiruVoidByte, MiruTenantId>>() {
            @Override
            public TenantIdAndRow<MiruVoidByte, MiruTenantId> callback(TenantIdAndRow<MiruVoidByte, MiruTenantId> r) throws Exception {
                if (r != null) {
                    tenantIds.add(r.getRow());
                }
                return r;
            }
        });
        return tenantIds;
    }
}
