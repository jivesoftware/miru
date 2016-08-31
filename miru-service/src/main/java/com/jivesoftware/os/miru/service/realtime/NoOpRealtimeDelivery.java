package com.jivesoftware.os.miru.service.realtime;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.realtime.MiruRealtimeDelivery;
import java.util.List;

/**
 *
 */
public class NoOpRealtimeDelivery implements MiruRealtimeDelivery {

    private final MiruStats miruStats;

    public NoOpRealtimeDelivery(MiruStats miruStats) {
        this.miruStats = miruStats;
    }

    @Override
    public void deliver(MiruPartitionCoord coord, List<Long> activityTimes) throws Exception {
        miruStats.egressed("realtime>delivery>noop", activityTimes.size(), 0);
    }
}
