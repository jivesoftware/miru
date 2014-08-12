package com.jivesoftware.os.miru.cluster.rcvs;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruActivityLookupTable;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.ConstantTimestamper;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 *
 */
@Singleton
public class MiruRCVSActivityLookupTable implements MiruActivityLookupTable {

    private final RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> lookupTable;

    @Inject
    public MiruRCVSActivityLookupTable(RowColumnValueStore<MiruVoidByte, MiruTenantId, Long, MiruActivityLookupEntry, ? extends Exception> lookupTable) {
        this.lookupTable = lookupTable;
    }

    @Override
    public MiruVersionedActivityLookupEntry getVersionedEntry(MiruTenantId tenantId, final long activityTimestamp) throws Exception {
        final AtomicReference<MiruVersionedActivityLookupEntry> entryRef = new AtomicReference<>();
        lookupTable.getEntrys(MiruVoidByte.INSTANCE, tenantId, activityTimestamp, 1L, 1, false, null, null,
            new CallbackStream<ColumnValueAndTimestamp<Long, MiruActivityLookupEntry, Long>>() {
                @Override
                public ColumnValueAndTimestamp<Long, MiruActivityLookupEntry, Long> callback(
                    ColumnValueAndTimestamp<Long, MiruActivityLookupEntry, Long> cvat) throws Exception {
                    if (cvat != null && cvat.getColumn() == activityTimestamp) {
                        entryRef.set(new MiruVersionedActivityLookupEntry(cvat.getTimestamp(), cvat.getValue()));
                    }
                    return cvat;
                }
            });
        return entryRef.get();
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

        lookupTable.getEntrys(MiruVoidByte.INSTANCE, tenantId, afterTimestamp, Long.MAX_VALUE, 1000, false, null, null,
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
}
