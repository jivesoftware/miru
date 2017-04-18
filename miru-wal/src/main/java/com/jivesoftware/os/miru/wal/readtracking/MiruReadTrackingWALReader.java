package com.jivesoftware.os.miru.wal.readtracking;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.routing.bird.shared.HostPort;

public interface MiruReadTrackingWALReader<C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> {

    C getCursor(long eventId);

    C stream(MiruTenantId tenantId,
        MiruStreamId streamId,
        C afterCursor,
        int batchSize,
        StreamReadTrackingWAL streamReadTrackingWAL) throws Exception;

    S streamSip(MiruTenantId tenantId,
        MiruStreamId streamId,
        S afterSipCursor,
        int batchSize,
        StreamReadTrackingSipWAL streamReadTrackingSipWAL) throws Exception;

    interface StreamReadTrackingWAL {

        boolean stream(long eventId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception;
    }

    interface StreamReadTrackingSipWAL {

        boolean stream(long eventId, long timestamp) throws Exception;
    }

}
