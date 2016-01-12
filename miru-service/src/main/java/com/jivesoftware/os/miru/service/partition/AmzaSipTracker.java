package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.ImmutableSet;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.topology.NamedCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.Set;

/**
 *
 */
public class AmzaSipTracker implements MiruSipTracker<AmzaSipCursor> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final Set<TimeAndVersion> seenLastSip = ImmutableSet.of();
    private final Set<TimeAndVersion> seenThisSip = ImmutableSet.of();

    public AmzaSipTracker() {
    }

    @Override
    public Set<TimeAndVersion> getSeenLastSip() {
        return seenLastSip;
    }

    @Override
    public Set<TimeAndVersion> getSeenThisSip() {
        return seenThisSip;
    }

    @Override
    public void track(MiruPartitionedActivity activity) {
    }

    @Override
    public AmzaSipCursor suggest(AmzaSipCursor lastSipCursor, AmzaSipCursor nextSipCursor) {
        return nextSipCursor;
    }

    @Override
    public boolean wasSeenLastSip(TimeAndVersion timeAndVersion) {
        return false;
    }

    @Override
    public void addSeenThisSip(TimeAndVersion timeAndVersion) {
    }

    @Override
    public void metrics(MiruPartitionCoord coord, AmzaSipCursor sip) {
        for (NamedCursor cursor : sip.cursors) {
            LOG.set(ValueType.COUNT, "sipTimestamp>partition>" + coord.partitionId + ">" + cursor.name,
                cursor.id, coord.tenantId.toString());
        }
    }
}
