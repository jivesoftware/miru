package com.jivesoftware.os.miru.wal.activity;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALRow;
import com.jivesoftware.os.rcvs.api.MultiAdd;
import com.jivesoftware.os.rcvs.api.RowColumValueTimestampAdd;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.timestamper.Timestamper;
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

    public static interface ActivityTimestamper {

        Timestamper get(MiruPartitionedActivity activity);
    }

    private final RowColumnValueStore<MiruTenantId, MiruActivityWALRow, C, MiruPartitionedActivity, ? extends Exception> wal;
    private final ColumnKey<C> columnKey;
    private final CollisionId collisionId;
    private final ActivityTimestamper timestamper;

    public MiruActivityWALWriterImpl(
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, C, MiruPartitionedActivity, ? extends Exception> wal,
        ColumnKey<C> columnKey,
        CollisionId collisionId,
        ActivityTimestamper timestamper) {
        this.wal = wal;
        this.columnKey = columnKey;
        this.collisionId = collisionId;
        this.timestamper = timestamper;
    }

    @Override
    public void write(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        MultiAdd<MiruActivityWALRow, C, MiruPartitionedActivity> rawAdds = new MultiAdd<>();
        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            rawAdds.add(
                new MiruActivityWALRow(partitionedActivity.partitionId.getId()),
                columnKey.provideColumnKey(partitionedActivity, collisionId),
                partitionedActivity,
                timestamper.get(partitionedActivity));
        }

        List<RowColumValueTimestampAdd<MiruActivityWALRow, C, MiruPartitionedActivity>> took = rawAdds.take();
        wal.multiRowsMultiAdd(tenantId, took);
    }

}
