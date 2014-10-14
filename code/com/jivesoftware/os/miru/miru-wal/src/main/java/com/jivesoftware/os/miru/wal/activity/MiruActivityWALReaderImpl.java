package com.jivesoftware.os.miru.wal.activity;

import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALRow;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import java.util.List;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableLong;

/** @author jonathan */
public class MiruActivityWALReaderImpl implements MiruActivityWALReader {

    private final RowColumnValueStore<MiruTenantId,
        MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL;
    private final RowColumnValueStore<MiruTenantId,
        MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL;

    public MiruActivityWALReaderImpl(
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity, ? extends Exception> activityWAL,
        RowColumnValueStore<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity, ? extends Exception> activitySipWAL) {

        this.activityWAL = activityWAL;
        this.activitySipWAL = activitySipWAL;
    }

    private MiruActivityWALRow rowKey(MiruPartitionId partition) {
        return new MiruActivityWALRow(partition.getId());
    }

    @Override
    public void stream(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        long afterTimestamp,
        final int batchSize,
        long sleepOnFailureMillis,
        StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception {

        MiruActivityWALRow rowKey = rowKey(partitionId);

        final List<ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long>> cvats = Lists.newArrayListWithCapacity(batchSize);
        final MutableBoolean streaming = new MutableBoolean(true);
        final MutableLong lastTimestamp = new MutableLong(afterTimestamp);
        while (streaming.booleanValue()) {
            try {
                MiruActivityWALColumnKey start = new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.ACTIVITY.getSort(), lastTimestamp.longValue());
                activityWAL.getEntrys(tenantId, rowKey, start, Long.MAX_VALUE, batchSize, false, null, null,
                    new CallbackStream<ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long>>() {
                        @Override
                        public ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long> callback(
                            ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long> v) throws Exception {

                            if (v != null) {
                                cvats.add(v);
                            }
                            if (cvats.size() < batchSize) {
                                return v;
                            } else {
                                return null;
                            }
                        }
                    });

                if (cvats.size() < batchSize) {
                    streaming.setValue(false);
                }
                for (ColumnValueAndTimestamp<MiruActivityWALColumnKey, MiruPartitionedActivity, Long> v : cvats) {
                    if (streamMiruActivityWAL.stream(v.getColumn().getCollisionId(), v.getValue(), v.getTimestamp())) {
                        // activityWAL is inclusive of the given timestamp, so add 1
                        lastTimestamp.setValue(v.getColumn().getCollisionId() + 1);
                    } else {
                        streaming.setValue(false);
                        break;
                    }
                }
                cvats.clear();
            } catch (Exception e) {
                try {
                    Thread.sleep(sleepOnFailureMillis);
                } catch (InterruptedException ie) {
                    Thread.interrupted();
                    throw new RuntimeException("Interrupted during retry after failure, expect partial results");
                }
            }
        }
    }

    @Override
    public void streamSip(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        long afterTimestamp,
        final int batchSize,
        long sleepOnFailureMillis,
        final StreamMiruActivityWAL streamMiruActivityWAL)
        throws Exception {

        MiruActivityWALRow rowKey = rowKey(partitionId);

        final List<ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long>> cvats = Lists.newArrayListWithCapacity(batchSize);
        final MutableBoolean streaming = new MutableBoolean(true);
        final MutableLong lastTimestamp = new MutableLong(afterTimestamp);
        while (streaming.booleanValue()) {
            try {
                MiruActivitySipWALColumnKey start = new MiruActivitySipWALColumnKey(MiruPartitionedActivity.Type.ACTIVITY.getSort(), lastTimestamp.longValue());
                activitySipWAL.getEntrys(tenantId, rowKey, start, Long.MAX_VALUE, batchSize, false, null, null,
                    new CallbackStream<ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long>>() {
                        @Override
                        public ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long> callback(
                            ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long> v) throws Exception {

                            if (v != null) {
                                cvats.add(v);
                            }
                            if (cvats.size() < batchSize) {
                                return v;
                            } else {
                                return null;
                            }
                        }
                    });

                if (cvats.size() < batchSize) {
                    streaming.setValue(false);
                }
                for (ColumnValueAndTimestamp<MiruActivitySipWALColumnKey, MiruPartitionedActivity, Long> v : cvats) {
                    if (streamMiruActivityWAL.stream(v.getColumn().getCollisionId(), v.getValue(), v.getTimestamp())) {
                        // activitySipWAL is exclusive of the given timestamp, so do NOT add 1
                        lastTimestamp.setValue(v.getColumn().getCollisionId());
                    } else {
                        streaming.setValue(false);
                        break;
                    }
                }
                cvats.clear();
            } catch (Exception e) {
                try {
                    Thread.sleep(sleepOnFailureMillis);
                } catch (InterruptedException ie) {
                    Thread.interrupted();
                    throw new RuntimeException("Interrupted during retry after failure, expect partial results");
                }
            }
        }
    }

    @Override
    public MiruPartitionedActivity findExisting(MiruTenantId tenantId, MiruPartitionId partitionId, MiruPartitionedActivity activity) throws Exception {
        return activityWAL.get(
            tenantId,
            new MiruActivityWALRow(partitionId.getId()),
            new MiruActivityWALColumnKey(MiruPartitionedActivity.Type.ACTIVITY.getSort(), activity.timestamp),
            null, null);
    }

}
