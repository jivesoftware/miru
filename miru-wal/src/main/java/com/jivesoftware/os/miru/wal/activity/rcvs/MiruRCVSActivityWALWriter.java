package com.jivesoftware.os.miru.wal.activity.rcvs;

import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.MultiAdd;
import com.jivesoftware.os.rcvs.api.RowColumValueTimestampAdd;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.rcvs.api.timestamper.ConstantTimestamper;
import com.jivesoftware.os.rcvs.api.timestamper.Timestamper;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan
 */
public class MiruRCVSActivityWALWriter implements MiruActivityWALWriter {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

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

    @Override
    public void copyPartition(MiruTenantId tenantId, MiruPartitionId from, MiruPartitionId to, int batchSize) throws Exception {
        MiruActivityWALRow fromRow = new MiruActivityWALRow(from.getId());
        MiruActivityWALRow toRow = new MiruActivityWALRow(to.getId());

        List<RowColumValueTimestampAdd<MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity>> batch = Lists.newArrayList();
        AtomicLong copied = new AtomicLong(0);
        activityWAL.getEntrys(tenantId, fromRow, null, null, batchSize, false, null, null,
            new CallbackStream<ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long>>() {
                @Override
                public ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long> callback
                    (ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long> value) throws Exception {
                    batch.add(new RowColumValueTimestampAdd<>(toRow, value.getColumn(), value.getValue(), new ConstantTimestamper(value.getTimestamp())));
                    if (batch.size() == batchSize) {
                        activityWAL.multiRowsMultiAdd(tenantId, batch);
                        long c = copied.addAndGet(batch.size());
                        batch.clear();
                        LOG.info("Finished copying {} activities from partition {} to {} for tenant {}", c, from, to, tenantId);
                    }
                    return null;
                }
            });
        if (!batch.isEmpty()) {
            activityWAL.multiRowsMultiAdd(tenantId, batch);
        }
        batch.clear();

        List<RowColumValueTimestampAdd<MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity>> sipBatch = Lists.newArrayList();
        copied.set(0);
        sipWAL.getEntrys(tenantId, fromRow, null, null, batchSize, false, null, null,
            new CallbackStream<ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long>>() {
                @Override
                public ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long> callback
                    (ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long> value) throws Exception {
                    sipBatch.add(new RowColumValueTimestampAdd<>(toRow, value.getColumn(), value.getValue(), new ConstantTimestamper(value.getTimestamp())));
                    if (sipBatch.size() == batchSize) {
                        sipWAL.multiRowsMultiAdd(tenantId, sipBatch);
                        long c = copied.addAndGet(sipBatch.size());
                        sipBatch.clear();
                        LOG.info("Finished copying {} sip activities from partition {} to {} for tenant {}", c, from, to, tenantId);
                    }
                    return null;
                }
            });
        if (!sipBatch.isEmpty()) {
            sipWAL.multiRowsMultiAdd(tenantId, sipBatch);
        }
        sipBatch.clear();
    }

    @Override
    public void removePartition(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        activityWAL.removeRow(tenantId, new MiruActivityWALRow(partitionId.getId()), null);
        sipWAL.removeRow(tenantId, new MiruActivityWALRow(partitionId.getId()), null);
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
