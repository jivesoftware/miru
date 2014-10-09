package com.jivesoftware.os.miru.wal.activity;

import com.google.common.base.Charsets;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALRow;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;

/** @author jonathan */
public class MiruActivityWALReaderImpl implements MiruActivityWALReader {

    private final RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL;
    private final RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL;

    public MiruActivityWALReaderImpl(
        RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL,
        RowColumnValueStore<TenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL) {

        this.activityWAL = activityWAL;
        this.activitySipWAL = activitySipWAL;
    }

    private MiruActivityWALRow rowKey(MiruPartitionId partition) {
        return new MiruActivityWALRow(partition.getId());
    }

    @Override
    public void stream(MiruTenantId tenantId, MiruPartitionId partitionId, long afterTimestamp, int batchSize, StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception {
        TenantId tenant = new TenantId(new String(tenantId.getBytes(), Charsets.UTF_8));
        MiruActivityWALRow rowKey = rowKey(partitionId);

        streamFromActivityWAL(tenant, rowKey, afterTimestamp, batchSize, streamMiruActivityWAL);
    }

    @Override
    public void streamSip(MiruTenantId tenantId, MiruPartitionId partitionId, long afterTimestamp, int batchSize, StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception {
        TenantId tenant = new TenantId(new String(tenantId.getBytes(), Charsets.UTF_8));
        MiruActivityWALRow rowKey = rowKey(partitionId);

        streamFromActivitySipWAL(tenant, rowKey, afterTimestamp, batchSize, streamMiruActivityWAL);
    }

    private void streamFromActivityWAL(TenantId tenantId, MiruActivityWALRow rowKey, long afterTimestamp,
        int batchSize,
        final StreamMiruActivityWAL streamMiruActivityWAL) throws Exception {

        MiruActivityWALColumnKey start = new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.ACTIVITY.getSort(), afterTimestamp);

        activityWAL.getEntrys(tenantId, rowKey, start, Long.MAX_VALUE, batchSize, false, null, null,
            new CallbackStream<ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long>>() {
                @Override
                public ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long> callback(
                    ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long> v) throws Exception {

                        if (v != null) {
                            if (!streamMiruActivityWAL.stream(v.getColumn().getCollisionId(), v.getValue(), v.getTimestamp())) {
                                return null;
                            }
                        }
                        return v;
                    }
            });
    }

    private void streamFromActivitySipWAL(TenantId tenantId, MiruActivityWALRow rowKey, long afterTimestamp,
        int batchSize,
        final StreamMiruActivityWAL streamMiruActivityWAL) throws Exception {

        MiruActivitySipWALColumnKey start = new MiruActivitySipWALColumnKey(MiruPartitionedActivity.Type.ACTIVITY.getSort(), afterTimestamp);

        activitySipWAL.getEntrys(tenantId, rowKey, start, Long.MAX_VALUE, batchSize, false, null, null,
            new CallbackStream<ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long>>() {
                @Override
                public ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long> callback(
                    ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long> v) throws Exception {

                        if (v != null) {
                            if (!streamMiruActivityWAL.stream(v.getColumn().getCollisionId(), v.getValue(), v.getTimestamp())) {
                                return null;
                            }
                        }
                        return v;
                    }
            });
    }

    @Override
    public MiruPartitionedActivity findExisting(MiruTenantId tenantId, MiruPartitionId partitionId, MiruPartitionedActivity activity) throws Exception {
        return activityWAL.get(
            new TenantId(new String(tenantId.getBytes(), Charsets.UTF_8)),
            new MiruActivityWALRow(partitionId.getId()),
            new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.ACTIVITY.getSort(), activity.timestamp),
            null, null);
    }

}
