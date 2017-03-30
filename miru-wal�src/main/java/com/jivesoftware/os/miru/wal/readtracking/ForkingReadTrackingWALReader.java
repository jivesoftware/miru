package com.jivesoftware.os.miru.wal.readtracking;

import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.routing.bird.shared.HostPort;

public class ForkingReadTrackingWALReader<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> implements MiruReadTrackingWALReader<C, S> {

    private final MiruReadTrackingWALReader<C, S> readWAL;
    private final MiruReadTrackingWALReader<?, ?> routingWAL;

    public ForkingReadTrackingWALReader(MiruReadTrackingWALReader<C, S> readWAL,
        MiruReadTrackingWALReader<?, ?> routingWAL) {
        this.readWAL = readWAL;
        this.routingWAL = routingWAL;
    }

    @Override
    public HostPort[] getRoutingGroup(MiruTenantId tenantId, MiruStreamId streamId, boolean createIfAbsent) throws Exception {
        return routingWAL.getRoutingGroup(tenantId, streamId, createIfAbsent);
    }

    @Override
    public C getCursor(long eventId) {
        return readWAL.getCursor(eventId);
    }

    @Override
    public C stream(MiruTenantId tenantId,
        MiruStreamId streamId,
        C afterCursor,
        int batchSize,
        StreamReadTrackingWAL streamReadTrackingWAL) throws Exception {
        return readWAL.stream(tenantId, streamId, afterCursor, batchSize, streamReadTrackingWAL);
    }

    @Override
    public S streamSip(MiruTenantId tenantId,
        MiruStreamId streamId,
        S afterSipCursor,
        int batchSize,
        StreamReadTrackingSipWAL streamReadTrackingSipWAL) throws Exception {
        return readWAL.streamSip(tenantId, streamId, afterSipCursor, batchSize, streamReadTrackingSipWAL);
    }
}
