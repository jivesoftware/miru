package com.jivesoftware.os.miru.wal.readtracking;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.routing.bird.shared.HostPort;

public interface MiruReadTrackingWALReader {

    HostPort[] getRoutingGroup(MiruTenantId tenantId, MiruStreamId streamId) throws Exception;

    void stream(MiruTenantId tenantId, MiruStreamId streamId, long afterEventId, StreamReadTrackingWAL streamReadTrackingWAL) throws Exception;

    void streamSip(MiruTenantId tenantId, MiruStreamId streamId, long sipTimestamp, StreamReadTrackingSipWAL streamReadTrackingSipWAL)
        throws Exception;

    interface StreamReadTrackingWAL {

        boolean stream(long eventId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception;
    }

    interface StreamReadTrackingSipWAL {

        boolean stream(long eventId, long timestamp) throws Exception;
    }

}
