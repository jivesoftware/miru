package com.jivesoftware.os.miru.wal.readtracking.rcvs;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.HostPortProvider;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.RCVSCursor;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.rcvs.api.ColumnValueAndTimestamp;
import com.jivesoftware.os.rcvs.api.RowColumnValueStore;
import com.jivesoftware.os.routing.bird.shared.HostPort;
import java.util.List;

public class RCVSReadTrackingWALReader implements MiruReadTrackingWALReader<RCVSCursor, RCVSSipCursor> {

    private final HostPortProvider hostPortProvider;
    private final RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception>
        readTrackingWAL;
    private final RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> readTrackingSipWAL;

    public RCVSReadTrackingWALReader(HostPortProvider hostPortProvider,
        RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity, ? extends Exception> readTrackingWAL,
        RowColumnValueStore<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long, ? extends Exception> readTrackingSipWAL) {
        this.hostPortProvider = hostPortProvider;
        this.readTrackingWAL = readTrackingWAL;
        this.readTrackingSipWAL = readTrackingSipWAL;
    }

    private MiruReadTrackingWALRow rowKey(MiruStreamId streamId) {
        return new MiruReadTrackingWALRow(streamId);
    }

    public HostPort[] getRoutingGroup(MiruTenantId tenantId, MiruStreamId streamId, boolean createIfAbsent) throws Exception {
        RowColumnValueStore.HostAndPort hostAndPort = readTrackingSipWAL.locate(tenantId, rowKey(streamId));
        int port = hostPortProvider.getPort(hostAndPort.host);
        if (port < 0) {
            return new HostPort[0];
        } else {
            return new HostPort[] { new HostPort(hostAndPort.host, port) };
        }
    }

    @Override
    public RCVSCursor getCursor(long eventId) {
        return new RCVSCursor(MiruPartitionedActivity.Type.ACTIVITY.getSort(), eventId, false, null);
    }

    @Override
    public long stream(MiruTenantId tenantId,
        MiruStreamId streamId,
        long id,
        StreamReadTrackingWAL streamReadTrackingWAL) throws Exception {

        return id;

        //TODO kill with fire
        /*final List<ColumnValueAndTimestamp<MiruReadTrackingWALColumnKey, MiruPartitionedActivity, Long>> cvats = Lists.newArrayListWithCapacity(batchSize);
        boolean streaming = true;
        if (afterCursor == null) {
            afterCursor = RCVSCursor.INITIAL;
        }

        byte nextSort = afterCursor.sort;
        long nextActivityTimestamp = afterCursor.activityTimestamp;
        boolean endOfStream = false;
        while (streaming) {
            MiruReadTrackingWALColumnKey start = new MiruReadTrackingWALColumnKey(nextActivityTimestamp);
            readTrackingWAL.getEntrys(tenantId, rowKey(streamId), start, Long.MAX_VALUE, batchSize, false, null, null,
                (ColumnValueAndTimestamp<MiruReadTrackingWALColumnKey, MiruPartitionedActivity, Long> v) -> {
                    if (v != null) {
                        if (!streamReadTrackingWAL.stream(v.getColumn().getEventId(), v.getValue(), v.getTimestamp())) {
                            return null;
                        }
                    }
                    return v;
                });

            if (cvats.size() < batchSize) {
                endOfStream = true;
                streaming = false;
            }
            for (ColumnValueAndTimestamp<MiruReadTrackingWALColumnKey, MiruPartitionedActivity, Long> v : cvats) {
                long eventId = v.getColumn().getEventId();
                if (streamReadTrackingWAL.stream(eventId, v.getValue(), v.getTimestamp())) {
                    // add 1 to exclude last result
                    if (eventId == Long.MAX_VALUE) {
                        nextActivityTimestamp = eventId;
                        endOfStream = true;
                        streaming = false;
                    } else {
                        nextActivityTimestamp = eventId + 1;
                    }
                } else {
                    streaming = false;
                    nextActivityTimestamp = eventId;
                    break;
                }
            }
            cvats.clear();
        }
        return new RCVSCursor(nextSort, nextActivityTimestamp, endOfStream, null);*/
    }

    @Override
    public RCVSSipCursor streamSip(MiruTenantId tenantId,
        MiruStreamId streamId,
        RCVSSipCursor afterCursor,
        int batchSize,
        StreamReadTrackingSipWAL streamReadTrackingSipWAL) throws Exception {

        final List<ColumnValueAndTimestamp<MiruReadTrackingSipWALColumnKey, Long, Long>> cvats = Lists.newArrayListWithCapacity(batchSize);
        boolean streaming = true;
        if (afterCursor == null) {
            afterCursor = RCVSSipCursor.INITIAL;
        }

        byte nextSort = afterCursor.sort;
        long nextClockTimestamp = afterCursor.clockTimestamp;
        long nextActivityTimestamp = afterCursor.activityTimestamp;
        boolean endOfStream = false;
        while (streaming) {
            MiruReadTrackingSipWALColumnKey start = new MiruReadTrackingSipWALColumnKey(nextClockTimestamp);
            readTrackingSipWAL.getEntrys(tenantId, rowKey(streamId), start, Long.MAX_VALUE, batchSize, false, null, null,
                (ColumnValueAndTimestamp<MiruReadTrackingSipWALColumnKey, Long, Long> v) -> {
                    if (v != null) {
                        cvats.add(v);
                    }
                    if (cvats.size() < batchSize) {
                        return v;
                    } else {
                        return null;
                    }
                });

            if (cvats.size() < batchSize) {
                streaming = false;
            }
            for (ColumnValueAndTimestamp<MiruReadTrackingSipWALColumnKey, Long, Long> v : cvats) {
                long sipId = v.getColumn().getSipId();
                long eventId = v.getColumn().getEventId().get();
                if (streamReadTrackingSipWAL.stream(eventId, v.getTimestamp())) {
                    // add 1 to exclude last result
                    if (eventId == Long.MAX_VALUE) {
                        if (sipId == Long.MAX_VALUE) {
                            nextClockTimestamp = sipId;
                            nextActivityTimestamp = eventId;
                            endOfStream = true;
                            streaming = false;
                        } else {
                            nextClockTimestamp = sipId + 1;
                            nextActivityTimestamp = Long.MIN_VALUE;
                        }
                    } else {
                        nextClockTimestamp = sipId;
                        nextActivityTimestamp = eventId + 1;
                    }
                } else {
                    streaming = false;
                    nextClockTimestamp = sipId;
                    nextActivityTimestamp = eventId;
                    break;
                }
            }
            cvats.clear();
        }
        return new RCVSSipCursor(nextSort, nextClockTimestamp, nextActivityTimestamp, endOfStream);
    }
}
