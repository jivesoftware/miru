package com.jivesoftware.os.miru.wal.readtracking.rcvs;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.rcvs.api.MultiAdd;
import com.jivesoftware.os.rcvs.api.RowColumValueTimestampAdd;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.timestamper.ConstantTimestamper;
import java.util.List;

public class RCVSReadTrackingWALWriter {

    private final RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey,
        MiruPartitionedActivity, ? extends Exception> readTrackingWAL;
    private final RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey,
        Long, ? extends Exception> readTrackingSipWAL;

    public RCVSReadTrackingWALWriter(
        RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception> readTrackingWAL,
        RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> readTrackingSipWAL) {

        this.readTrackingWAL = readTrackingWAL;
        this.readTrackingSipWAL = readTrackingSipWAL;
    }

    public void write(MiruTenantId tenantId, MiruStreamId streamId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        writeReadTracking(tenantId, streamId, partitionedActivities);
        writeReadTrackingSip(tenantId, streamId, partitionedActivities);
    }

    private void writeReadTracking(MiruTenantId tenantId, MiruStreamId streamId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        MultiAdd<MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity> rawAdds = new MultiAdd<>();
        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            MiruReadEvent readEventNullable = partitionedActivity.getReadEventNullable();
            if (readEventNullable != null && readEventNullable.streamId.equals(streamId)) {
                rawAdds.add(
                    new MiruReadTrackingWALRow(readEventNullable.streamId),
                    new MiruReadTrackingWALColumnKey(partitionedActivity.timestamp),
                    partitionedActivity,
                    new ConstantTimestamper(partitionedActivity.timestamp)
                );
            }
        }

        List<RowColumValueTimestampAdd<MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity>> took = rawAdds.take();
        readTrackingWAL.multiRowsMultiAdd(tenantId, took);
    }

    private void writeReadTrackingSip(MiruTenantId tenantId, MiruStreamId streamId, List<MiruPartitionedActivity> partitionedActivities) throws Exception {
        MultiAdd<MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long> rawAdds = new MultiAdd<>();
        for (MiruPartitionedActivity partitionedActivity : partitionedActivities) {
            MiruReadEvent readEventNullable = partitionedActivity.getReadEventNullable();
            if (readEventNullable != null && readEventNullable.streamId.equals(streamId)) {
                rawAdds.add(
                    new MiruReadTrackingWALRow(readEventNullable.streamId),
                    new MiruReadTrackingSipWALColumnKey(System.currentTimeMillis(), partitionedActivity.timestamp),
                    partitionedActivity.timestamp,
                    new ConstantTimestamper(System.currentTimeMillis())
                );
            }
        }

        List<RowColumValueTimestampAdd<MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long>> took = rawAdds.take();
        readTrackingSipWAL.multiRowsMultiAdd(tenantId, took);
    }
}
