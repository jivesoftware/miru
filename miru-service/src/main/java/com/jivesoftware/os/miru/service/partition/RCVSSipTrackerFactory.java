package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class RCVSSipTrackerFactory implements MiruSipTrackerFactory<RCVSSipCursor> {

    @Override
    public MiruSipTracker<RCVSSipCursor> create(Set<TimeAndVersion> seenLastSip) {
        final int maxSipReplaySize = 100; //TODO config
        long maxSipClockSkew = TimeUnit.SECONDS.toMillis(10); //TODO config
        return new RCVSSipTracker(maxSipReplaySize, maxSipClockSkew, seenLastSip);
    }
}
