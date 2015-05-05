package com.jivesoftware.os.miru.wal.readtracking;

import com.jivesoftware.os.jive.utils.base.interfaces.CallbackStream;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.MiruReadTrackingSipWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.MiruReadTrackingWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.MiruReadTrackingWALRow;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;

public class MiruReadTrackingWALReaderImpl implements MiruReadTrackingWALReader {

    private final RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception>
        readTrackingWAL;
    private final RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> readTrackingSipWAL;

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
}
