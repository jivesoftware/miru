package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import java.util.Set;

/**
 *
 */
public class AmzaSipTrackerFactory implements MiruSipTrackerFactory<AmzaSipCursor> {

    @Override
    public MiruSipTracker<AmzaSipCursor> create(Set<TimeAndVersion> seenLastSip) {
        return new AmzaSipTracker(seenLastSip);
    }
}
