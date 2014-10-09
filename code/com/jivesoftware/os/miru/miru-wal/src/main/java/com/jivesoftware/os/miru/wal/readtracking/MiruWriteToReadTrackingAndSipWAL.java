package com.jivesoftware.os.miru.wal.readtracking;

import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALWriterImpl.CollisionId;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALWriterImpl.ColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALWriterImpl.ColumnValue;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingSipWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALRow;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import java.util.List;

public class MiruWriteToReadTrackingAndSipWAL implements MiruReadTrackingWALWriter {
    private final MiruReadTrackingWALWriter readTrackingWALWriter;
    private final MiruReadTrackingWALWriter readTrackingSipWALWriter;

    public MiruWriteToReadTrackingAndSipWAL(
        RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception> readTrackingWAL,
        RowColumnValueStore<TenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> readTrackingSipWAL) {

        this.readTrackingWALWriter = new MiruReadTrackingWALWriterImpl<>(readTrackingWAL,
            READTRACKING_WAL_COLUMN_KEY,
            READTRACKING_WAL_COLUMN_VALUE,
            READTRACKING_WAL_COLLISIONID);

        this.readTrackingSipWALWriter = new MiruReadTrackingWALWriterImpl<>(readTrackingSipWAL,
            READTRACKING_SIP_WAL_COLUMN_KEY,
            READTRACKING_SIP_WAL_COLUMN_VALUE,
            READTRACKING_SIP_WAL_COLLISIONID);
    }

    @Override
    public void write(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        readTrackingWALWriter.write(tenantId, partitionedActivities);
        readTrackingSipWALWriter.write(tenantId, partitionedActivities);
    }

    // ReadTrackingWAL
    private static final ColumnKey<MiruReadTrackingWALColumnKey> READTRACKING_WAL_COLUMN_KEY = new ColumnKey<MiruReadTrackingWALColumnKey>() {
        @Override
        public MiruReadTrackingWALColumnKey provideColumnKey(MiruPartitionedActivity activity, CollisionId collisionId) {
            return new MiruReadTrackingWALColumnKey(collisionId.collisionId(activity));
        }
    };

    private static final ColumnValue<MiruPartitionedActivity> READTRACKING_WAL_COLUMN_VALUE = new ColumnValue<MiruPartitionedActivity>() {
        @Override
        public MiruPartitionedActivity provideColumnValue(MiruPartitionedActivity activity) {
            return activity;
        }
    };

    private static final CollisionId READTRACKING_WAL_COLLISIONID = new CollisionId() {
        @Override
        public long collisionId(MiruPartitionedActivity activity) {
            return activity.timestamp;
        }
    };

    // ReadTrackingSipWAL
    private static final ColumnKey<MiruReadTrackingSipWALColumnKey> READTRACKING_SIP_WAL_COLUMN_KEY = new ColumnKey<MiruReadTrackingSipWALColumnKey>() {
        @Override
        public MiruReadTrackingSipWALColumnKey provideColumnKey(MiruPartitionedActivity activity, CollisionId collisionId) {
            return new MiruReadTrackingSipWALColumnKey(collisionId.collisionId(activity), activity.timestamp);
        }
    };

    private static final ColumnValue<Long> READTRACKING_SIP_WAL_COLUMN_VALUE = new ColumnValue<Long>() {
        @Override
        public Long provideColumnValue(MiruPartitionedActivity activity) {
            return activity.timestamp;
        }
    };

    private static final CollisionId READTRACKING_SIP_WAL_COLLISIONID = new CollisionId() {
        @Override
        public long collisionId(MiruPartitionedActivity activity) {
            return System.currentTimeMillis();
        }
    };
}
