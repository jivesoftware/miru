package com.jivesoftware.os.miru.wal.readtracking;

import com.google.common.base.Charsets;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.MultiAdd;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumValueTimestampAdd;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.RowColumnValueStore;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.ConstantTimestamper;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALRow;
import java.util.List;

public class MiruReadTrackingWALWriterImpl<C, V> implements MiruReadTrackingWALWriter {

    public static interface CollisionId {

        long collisionId(MiruPartitionedActivity activity);
    }

    public static interface ColumnKey<C> {

        C provideColumnKey(MiruPartitionedActivity activity, CollisionId collisionId);
    }

    public static interface ColumnValue<V> {

        V provideColumnValue(MiruPartitionedActivity activity);
    }

    private final RowColumnValueStore<TenantId, MiruReadTrackingWALRow, C, V, ? extends Exception> wal;
    private final ColumnKey<C> columnKey;
    private final ColumnValue<V> columnValue;
    private final CollisionId collisionId;

    public MiruReadTrackingWALWriterImpl(RowColumnValueStore<TenantId, MiruReadTrackingWALRow, C, V, ? extends Exception> wal,
        ColumnKey<C> columnKey, ColumnValue<V> columnValue, CollisionId collisionId) {
        this.wal = wal;
        this.columnKey = columnKey;
        this.columnValue = columnValue;
        this.collisionId = collisionId;
    }

    @Override
    public void write(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        MultiAdd<MiruReadTrackingWALRow, C, V> rawAdds = new MultiAdd<>();
        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            MiruReadEvent readEventNullable = partitionedActivity.getReadEventNullable();
            if (readEventNullable != null) {
                rawAdds.add(
                    new MiruReadTrackingWALRow(readEventNullable.streamId),
                    columnKey.provideColumnKey(partitionedActivity, collisionId),
                    columnValue.provideColumnValue(partitionedActivity),
                    new ConstantTimestamper(collisionId.collisionId(partitionedActivity))
                );
            }
        }

        List<RowColumValueTimestampAdd<MiruReadTrackingWALRow, C, V>> took = rawAdds.take();
        wal.multiRowsMultiAdd(new TenantId(new String(tenantId.getBytes(), Charsets.UTF_8)), took);
    }
}
