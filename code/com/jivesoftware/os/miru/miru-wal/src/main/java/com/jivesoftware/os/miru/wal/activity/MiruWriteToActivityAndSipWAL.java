package com.jivesoftware.os.miru.wal.activity;

import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriterImpl.CollisionId;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriterImpl.ColumnKey;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALRow;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import java.util.List;

/** @author jonathan */
public class MiruWriteToActivityAndSipWAL implements MiruActivityWALWriter {
    private final MiruActivityWALWriter activityWALWriter;
    private final MiruActivityWALWriter activitySipWALWriter;

    public MiruWriteToActivityAndSipWAL(
        RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL,
        RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL) {

        this.activityWALWriter = new MiruActivityWALWriterImpl<>(activityWAL,
            ACTIVITY_WAL_COLUMN_KEY,
            ACTIVITY_WAL_COLLISIONID);

        this.activitySipWALWriter = new MiruActivityWALWriterImpl<>(activitySipWAL,
            ACTIVITY_SIP_WAL_COLUMN_KEY,
            ACTIVITY_SIP_WAL_COLLISIONID);
    }

    @Override
    public void write(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        activityWALWriter.write(tenantId, partitionedActivities);
        activitySipWALWriter.write(tenantId, partitionedActivities);
    }

    // ActivityWAL
    private static final ColumnKey<MiruActivityWALColumnKey> ACTIVITY_WAL_COLUMN_KEY = new ColumnKey<MiruActivityWALColumnKey>() {
        @Override
        public MiruActivityWALColumnKey provideColumnKey(MiruPartitionedActivity activity, CollisionId collisionId) {
            return new MiruActivityWALColumnKey(activity.type.getSort(), collisionId.collisionId(activity));
        }
    };

    private static final CollisionId ACTIVITY_WAL_COLLISIONID = new CollisionId() {
        @Override
        public long collisionId(MiruPartitionedActivity activity) {
            if (activity.type != MiruPartitionedActivity.Type.BEGIN && activity.type != MiruPartitionedActivity.Type.END) {
                return activity.timestamp;
            } else {
                return activity.writerId;
            }
        }
    };

    // ActivitySipWAL
    private static final ColumnKey<MiruActivitySipWALColumnKey> ACTIVITY_SIP_WAL_COLUMN_KEY = new ColumnKey<MiruActivitySipWALColumnKey>() {
        @Override
        public MiruActivitySipWALColumnKey provideColumnKey(MiruPartitionedActivity activity, CollisionId collisionId) {
            return new MiruActivitySipWALColumnKey(activity.type.getSort(), collisionId.collisionId(activity), activity.timestamp);
        }
    };

    private static final CollisionId ACTIVITY_SIP_WAL_COLLISIONID = new CollisionId() {
        @Override
        public long collisionId(MiruPartitionedActivity activity) {
            if (activity.type != MiruPartitionedActivity.Type.BEGIN && activity.type != MiruPartitionedActivity.Type.END) {
                return activity.clockTimestamp;
            } else {
                return activity.writerId;
            }
        }
    };
}
