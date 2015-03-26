package com.jivesoftware.os.miru.wal.activity.rcvs;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.rcvs.api.MultiAdd;
import com.jivesoftware.os.rcvs.api.RowColumValueTimestampAdd;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.timestamper.ConstantTimestamper;
import com.jivesoftware.os.rcvs.api.timestamper.Timestamper;
import java.util.List;

/**
 * @author jonathan
 */
public class MiruRCVSActivityWALWriter implements MiruActivityWALWriter {

    private final RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL;
    private final RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> sipWAL;

    public MiruRCVSActivityWALWriter(
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL,
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> sipWAL) {
        this.activityWAL = activityWAL;
        this.sipWAL = sipWAL;
    }

    @Override
    public void write(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        writeActivity(tenantId, partitionedActivities);
        writeSip(tenantId, partitionedActivities);
    }

    private void writeActivity(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        MultiAdd<MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity> rawActivities = new MultiAdd<>();
        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            long activityCollisionId;
            if (partitionedActivity.type != MiruPartitionedActivity.Type.BEGIN && partitionedActivity.type != MiruPartitionedActivity.Type.END) {
                activityCollisionId = partitionedActivity.timestamp;
            } else {
                activityCollisionId = partitionedActivity.writerId;
            }

            Timestamper timestamper = partitionedActivity.activity.isPresent()
                ? new ConstantTimestamper(partitionedActivity.activity.get().version)
                : null;

            rawActivities.add(
                new MiruActivityWALRow(partitionedActivity.partitionId.getId()),
                new MiruActivityWALColumnKey(partitionedActivity.type.getSort(), activityCollisionId),
                partitionedActivity,
                timestamper);
        }

        List<RowColumValueTimestampAdd<MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity>> tookActivities = rawActivities.take();
        activityWAL.multiRowsMultiAdd(tenantId, tookActivities);
    }

    private void writeSip(MiruTenantId tenantId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        MultiAdd<MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity> rawSips = new MultiAdd<>();
        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            long sipCollisionId;
            if (partitionedActivity.type != MiruPartitionedActivity.Type.BEGIN && partitionedActivity.type != MiruPartitionedActivity.Type.END) {
                sipCollisionId = partitionedActivity.clockTimestamp;
            } else {
                sipCollisionId = partitionedActivity.writerId;
            }

            rawSips.add(
                new MiruActivityWALRow(partitionedActivity.partitionId.getId()),
                new MiruActivitySipWALColumnKey(partitionedActivity.type.getSort(), sipCollisionId, partitionedActivity.timestamp),
                partitionedActivity,
                null);
        }

        List<RowColumValueTimestampAdd<MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity>> tookSips = rawSips.take();
        sipWAL.multiRowsMultiAdd(tenantId, tookSips);
    }

}
