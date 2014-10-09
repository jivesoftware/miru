package com.jivesoftware.os.miru.wal.activity;

import com.google.common.base.Charsets;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALRow;
import com.jivesoftware.os.rcvs.api.MultiAdd;
import com.jivesoftware.os.rcvs.api.RowColumValueTimestampAdd;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.timestamper.ConstantTimestamper;
import java.util.List;

/**
 * @author jonathan
 */
public class MiruActivityWALWriterImpl<C> implements MiruActivityWALWriter {

    public static interface CollisionId {

        long collisionId(MiruPartitionedActivity activity);
    }

    public static interface ColumnKey<C> {

        C provideColumnKey(MiruPartitionedActivity activity, CollisionId collisionId);
    }

    private final RowColumnValueStore<TenantId, MiruActivityWALRow, C, MiruPartitionedActivity, ? extends Exception> wal;
    private final ColumnKey<C> columnKey;
    private final CollisionId collisionId;

    public MiruActivityWALWriterImpl(
        RowColumnValueStore<TenantId, MiruActivityWALRow, C, MiruPartitionedActivity, ? extends Exception> wal,
        ColumnKey<C> columnKey,
        CollisionId collisionId) {
        this.wal = wal;
        this.columnKey = columnKey;
        this.collisionId = collisionId;
    }

    @Override
    public void write(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        MultiAdd<MiruActivityWALRow, C, MiruPartitionedActivity> rawAdds = new MultiAdd<>();
        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            rawAdds.add(
                new MiruActivityWALRow(partitionedActivity.partitionId.getId()),
                columnKey.provideColumnKey(partitionedActivity, collisionId),
                partitionedActivity,
                partitionedActivity.activity.isPresent()
                    ? new ConstantTimestamper(partitionedActivity.activity.get().version)
                    : null // CurrentTimestamper
            );
        }

        List<RowColumValueTimestampAdd<MiruActivityWALRow, C, MiruPartitionedActivity>> took = rawAdds.take();
        wal.multiRowsMultiAdd(new TenantId(new String(tenantId.getBytes(), Charsets.UTF_8)), took);
    }

}
