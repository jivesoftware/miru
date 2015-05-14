package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import java.util.Set;

/**
 *
 */
public interface MiruSipTrackerFactory<S extends MiruSipCursor<S>> {

    MiruSipTracker<S> create(Set<TimeAndVersion> seenLastSip);
}
