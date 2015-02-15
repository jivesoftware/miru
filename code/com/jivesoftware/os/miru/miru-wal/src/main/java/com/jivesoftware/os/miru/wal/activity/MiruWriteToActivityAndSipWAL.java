package com.jivesoftware.os.miru.wal.activity;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriterImpl.ActivityTimestamper;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriterImpl.CollisionId;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriterImpl.ColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruActivityWALRow;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.timestamper.ConstantTimestamper;
import com.jivesoftware.os.rcvs.api.timestamper.Timestamper;
import java.util.List;

/** @author jonathan */
public class MiruWriteToActivityAndSipWAL implements MiruActivityWALWriter {
    private final MiruActivityWALWriter activityWALWriter;
    private final MiruActivityWALWriter activitySipWALWriter;

    public MiruWriteToActivityAndSipWAL(
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL,
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL) {

        this.activityWALWriter = new MiruActivityWALWriterImpl<>(activityWAL,
            ACTIVITY_WAL_COLUMN_KEY,
            ACTIVITY_WAL_COLLISIONID,
            ACTIVITY_WAL_TIMESTAMPER);

        this.activitySipWALWriter = new MiruActivityWALWriterImpl<>(activitySipWAL,
            ACTIVITY_SIP_WAL_COLUMN_KEY,
            ACTIVITY_SIP_WAL_COLLISIONID,
            ACTIVITY_SIP_WAL_TIMESTAMPER);
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

    private static final ActivityTimestamper ACTIVITY_WAL_TIMESTAMPER = new ActivityTimestamper() {
        @Override
        public Timestamper get(MiruPartitionedActivity partitionedActivity) {
            return partitionedActivity.activity.isPresent()
                ? new ConstantTimestamper(partitionedActivity.activity.get().version)
                : null; // CurrentTimestamper
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

    private static final ActivityTimestamper ACTIVITY_SIP_WAL_TIMESTAMPER = new ActivityTimestamper() {
        @Override
        public Timestamper get(MiruPartitionedActivity partitionedActivity) {
            return null; // CurrentTimestamper
        }
    };
}
