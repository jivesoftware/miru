package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.TimeAndVersion;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class RCVSSipTracker implements MiruSipTracker<RCVSSipCursor> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final long maxSipClockSkew;

    private final byte[] sorts;
    private final long[] clockTimestamps;
    private final long[] activityTimestamps;
    private final AtomicInteger index;

    private final Set<TimeAndVersion> seenLastSip;
    private final Set<TimeAndVersion> seenThisSip;

    public RCVSSipTracker(int maxSipReplaySize, long maxSipClockSkew, Set<TimeAndVersion> seenLastSip) {
        this.maxSipClockSkew = maxSipClockSkew;

        this.sorts = new byte[maxSipReplaySize];
        this.clockTimestamps = new long[maxSipReplaySize];
        this.activityTimestamps = new long[maxSipReplaySize];
        this.index = new AtomicInteger();

        this.seenLastSip = seenLastSip;
        this.seenThisSip = Sets.newHashSet();
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
        if (!activity.type.isBoundaryType()) {
            int i = index.getAndIncrement() % clockTimestamps.length;
            sorts[i] = activity.type.getSort();
            clockTimestamps[i] = activity.clockTimestamp;
            activityTimestamps[i] = activity.timestamp;
        }
    }

    @Override
    public RCVSSipCursor suggest(RCVSSipCursor lastSipCursor, RCVSSipCursor nextSipCursor) {
        int lastIndex = index.get() - 1;
        if (lastIndex < 0) {
            return lastSipCursor;
        }

        boolean endOfStream = nextSipCursor != null ? nextSipCursor.endOfStream : lastSipCursor.endOfStream;
        byte latestSort = sorts[lastIndex % clockTimestamps.length];
        long latestTimestamp = clockTimestamps[lastIndex % clockTimestamps.length];
        long latestMinusSkew = latestTimestamp - maxSipClockSkew;
        if (lastIndex < clockTimestamps.length) {
            // fewer than the max replay size, so sip to the more distant timestamp
            if (clockTimestamps[0] < latestMinusSkew) {
                return new RCVSSipCursor(sorts[0], clockTimestamps[0], activityTimestamps[0], endOfStream);
            } else {
                return new RCVSSipCursor(latestSort, latestMinusSkew, 0, endOfStream);
            }
        } else {
            // more than the max replay size, so sip to the more recent timestamp
            int oldestIndex = index.get() % clockTimestamps.length;
            if (clockTimestamps[oldestIndex] > latestMinusSkew) {
                return new RCVSSipCursor(sorts[oldestIndex], clockTimestamps[oldestIndex], activityTimestamps[oldestIndex], endOfStream);
            } else {
                return new RCVSSipCursor(latestSort, latestMinusSkew, 0, endOfStream);
            }
        }
    }

    @Override
    public boolean wasSeenLastSip(TimeAndVersion timeAndVersion) {
        return seenLastSip.contains(timeAndVersion);
    }

    @Override
    public void addSeenThisSip(TimeAndVersion timeAndVersion) {
        seenThisSip.add(timeAndVersion);
    }

    @Override
    public void metrics(MiruPartitionCoord coord, RCVSSipCursor sip) {
        LOG.set(ValueType.COUNT, "sipTimestamp>partition>" + coord.partitionId + ">sort",
            sip.sort, coord.tenantId.toString());
        LOG.set(ValueType.COUNT, "sipTimestamp>partition>" + coord.partitionId + ">clock",
            sip.clockTimestamp, coord.tenantId.toString());
        LOG.set(ValueType.COUNT, "sipTimestamp>partition>" + coord.partitionId + ">activity",
            sip.activityTimestamp, coord.tenantId.toString());

    }
}
