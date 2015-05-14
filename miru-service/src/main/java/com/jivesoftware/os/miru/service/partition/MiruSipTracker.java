package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import java.util.Set;

/**
 *
 */
public interface MiruSipTracker<S extends MiruSipCursor<S>> {

    Set<TimeAndVersion> getSeenLastSip();

    Set<TimeAndVersion> getSeenThisSip();

    void track(MiruPartitionedActivity activity);

    S suggest(S lastSipCursor, S nextSipCursor);

    boolean wasSeenLastSip(TimeAndVersion timeAndVersion);

    void addSeenThisSip(TimeAndVersion timeAndVersion);

    void metrics(MiruPartitionCoord coord, S sip);
}
