package com.jivesoftware.os.miru.wal.readtracking.rcvs;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.routing.bird.shared.HostPort;

public class RCVSReadTrackingWALReader implements MiruReadTrackingWALReader {

    private final RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception>
        readTrackingWAL;
    private final RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> readTrackingSipWAL;

    public RCVSReadTrackingWALReader(
        RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception> readTrackingWAL,
        RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> readTrackingSipWAL) {

        this.readTrackingWAL = readTrackingWAL;
        this.readTrackingSipWAL = readTrackingSipWAL;
    }

    private MiruReadTrackingWALRow rowKey(MiruStreamId streamId) {
        return new MiruReadTrackingWALRow(streamId);
    }

    @Override
    public HostPort[] getRoutingGroup(MiruTenantId tenantId, MiruStreamId streamId) throws Exception {
        RowColumnValueStore.HostAndPort hostAndPort = readTrackingSipWAL.locate(tenantId, rowKey(streamId));
        return new HostPort[] { new HostPort(hostAndPort.host, hostAndPort.port) };
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
            (ColumnValueAndTimestamp<MiruReadTrackingWALColumnKey, MiruPartitionedActivity, Long> v) -> {
                if (v != null) {
                    if (!streamReadTrackingWAL.stream(v.getColumn().getEventId(), v.getValue(), v.getTimestamp())) {
                        return null;
                    }
                }
                return v;
            });
    }

    private void streamFromReadTrackingSipWAL(MiruTenantId tenantId, MiruReadTrackingWALRow rowKey, long afterTimestamp,
        final StreamReadTrackingSipWAL streamReadTrackingSipWAL) throws Exception {

        MiruReadTrackingSipWALColumnKey start = new MiruReadTrackingSipWALColumnKey(afterTimestamp);

        readTrackingSipWAL.getEntrys(tenantId, rowKey, start, Long.MAX_VALUE, 1_000, false, null, null,
            (ColumnValueAndTimestamp<MiruReadTrackingSipWALColumnKey, Long, Long> v) -> {
                if (v != null) {
                    if (!streamReadTrackingSipWAL.stream(v.getValue(), v.getTimestamp())) {
                        return null;
                    }
                }
                return v;
            });
    }
}
