package com.jivesoftware.os.miru.wal.readtracking;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.MiruReadTrackingSipWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.MiruReadTrackingWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.MiruReadTrackingWALRow;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MiruReadTrackingWALReaderImpl implements MiruReadTrackingWALReader {

    private final RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception>
        readTrackingWAL;
    private final RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception>
        readTrackingSipWAL;

    // TODO - this should probably live in the context
    private final Map<MiruTenantPartitionAndStreamId, Long> userSipTimestamp = new ConcurrentHashMap<>();

    public MiruReadTrackingWALReaderImpl(
        RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception> readTrackingWAL,
        RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> readTrackingSipWAL) {

        this.readTrackingWAL = readTrackingWAL;
        this.readTrackingSipWAL = readTrackingSipWAL;
    }

    private MiruReadTrackingWALRow rowKey(MiruStreamId streamId) {
        return new MiruReadTrackingWALRow(streamId);
    }

    @Override
    public void stream(MiruTenantId tenantId, MiruStreamId streamId, long afterEventId, StreamReadTrackingWAL streamReadTrackingWAL) throws Exception {
        streamFromReadTrackingWAL(tenantId, rowKey(streamId), afterEventId, streamReadTrackingWAL);
    }

    @Override
    public void streamSip(MiruTenantId tenantId, MiruStreamId streamId, long sipTimestamp, StreamReadTrackingSipWAL streamReadTrackingSipWAL)
        throws Exception {

        streamFromReadTrackingSipWAL(tenantId, rowKey(streamId), sipTimestamp, streamReadTrackingSipWAL);
    }

    @Override
    public void setSipTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId, MiruStreamId streamId, long sipTimestamp) {
        userSipTimestamp.put(new MiruTenantPartitionAndStreamId(tenantId, partitionId, streamId), sipTimestamp);
    }

    @Override
    public long getSipTimestamp(MiruTenantId tenantId, MiruPartitionId partitionId, MiruStreamId streamId) {
        Long sipTimestamp = userSipTimestamp.get(new MiruTenantPartitionAndStreamId(tenantId, partitionId, streamId));
        return sipTimestamp != null ? sipTimestamp : 0;
    }

    private void streamFromReadTrackingWAL(MiruTenantId tenantId, MiruReadTrackingWALRow rowKey, long afterEventId,
        final StreamReadTrackingWAL streamReadTrackingWAL) throws Exception {

        MiruReadTrackingWALColumnKey start = new MiruReadTrackingWALColumnKey(afterEventId);

        readTrackingWAL.getEntrys(tenantId, rowKey, start, Long.MAX_VALUE, 1_000, false, null, null,
            new CallbackStream<ColumnValueAndTimestamp<MiruReadTrackingWALColumnKey, MiruPartitionedActivity, Long>>() {
                @Override
                public ColumnValueAndTimestamp<MiruReadTrackingWALColumnKey, MiruPartitionedActivity, Long> callback(
                    ColumnValueAndTimestamp<MiruReadTrackingWALColumnKey, MiruPartitionedActivity, Long> v) throws Exception {

                    if (v != null) {
                        if (!streamReadTrackingWAL.stream(v.getColumn().getEventId(), v.getValue(), v.getTimestamp())) {
                            return null;
                        }
                    }
                    return v;
                }
            }
        );
    }

    private void streamFromReadTrackingSipWAL(MiruTenantId tenantId, MiruReadTrackingWALRow rowKey, long afterTimestamp,
        final StreamReadTrackingSipWAL streamReadTrackingSipWAL) throws Exception {

        MiruReadTrackingSipWALColumnKey start = new MiruReadTrackingSipWALColumnKey(afterTimestamp);

        readTrackingSipWAL.getEntrys(tenantId, rowKey, start, Long.MAX_VALUE, 1_000, false, null, null,
            new CallbackStream<ColumnValueAndTimestamp<MiruReadTrackingSipWALColumnKey, Long, Long>>() {
                @Override
                public ColumnValueAndTimestamp<MiruReadTrackingSipWALColumnKey, Long, Long> callback(
                    ColumnValueAndTimestamp<MiruReadTrackingSipWALColumnKey, Long, Long> v) throws Exception {

                    if (v != null) {
                        if (!streamReadTrackingSipWAL.stream(v.getValue(), v.getTimestamp())) {
                            return null;
                        }
                    }
                    return v;
                }
            }
        );
    }

    private class MiruTenantPartitionAndStreamId {
        private final MiruTenantId tenantId;
        private final MiruPartitionId partitionId;
        private final MiruStreamId streamId;

        private MiruTenantPartitionAndStreamId(MiruTenantId tenantId, MiruPartitionId partitionId, MiruStreamId streamId) {
            this.tenantId = tenantId;
            this.partitionId = partitionId;
            this.streamId = streamId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MiruTenantPartitionAndStreamId that = (MiruTenantPartitionAndStreamId) o;

            if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null) {
                return false;
            }
            if (streamId != null ? !streamId.equals(that.streamId) : that.streamId != null) {
                return false;
            }
            return !(tenantId != null ? !tenantId.equals(that.tenantId) : that.tenantId != null);
        }

        @Override
        public int hashCode() {
            int result = tenantId != null ? tenantId.hashCode() : 0;
            result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
            result = 31 * result + (streamId != null ? streamId.hashCode() : 0);
            return result;
        }
    }
}