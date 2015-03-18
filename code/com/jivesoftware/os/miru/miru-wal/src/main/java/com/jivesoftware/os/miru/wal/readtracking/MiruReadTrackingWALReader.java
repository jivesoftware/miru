package com.jivesoftware.os.miru.wal.readtracking;

import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;

public interface MiruReadTrackingWALReader {

    public static interface StreamReadTrackingWAL {

        boolean stream(long eventId, MiruPartitionedActivity partitionedActivity, long timestamp) throws Exception;
    }

    public static interface StreamReadTrackingSipWAL {

        boolean stream(long eventId, long timestamp) throws Exception;
    }

    public void stream(MiruTenantId tenantId, MiruStreamId streamId, long afterEventId, StreamReadTrackingWAL streamReadTrackingWAL) throws Exception;

    void streamSip(MiruTenantId tenantId, MiruStreamId streamId, long sipTimestamp, StreamReadTrackingSipWAL streamReadTrackingSipWAL)
        throws Exception;

}
