package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader.Sip;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class MiruSipTracker {

    private final long maxSipClockSkew;

    private final long[] clockTimestamps;
    private final long[] activityTimestamps;
    private final AtomicInteger index;

    private final Set<TimeAndVersion> seenLastSip;
    private final Set<TimeAndVersion> seenThisSip;

    public MiruSipTracker(int maxSipReplaySize, long maxSipClockSkew, Set<TimeAndVersion> seenLastSip) {
        this.maxSipClockSkew = maxSipClockSkew;

        this.clockTimestamps = new long[maxSipReplaySize];
        this.activityTimestamps = new long[maxSipReplaySize];
        this.index = new AtomicInteger();

        this.seenLastSip = seenLastSip;
        this.seenThisSip = Sets.newHashSet();
    }

    public Set<TimeAndVersion> getSeenLastSip() {
        return seenLastSip;
    }

    public Set<TimeAndVersion> getSeenThisSip() {
        return seenThisSip;
    }

    public void put(long clockTimestamp, long activityTimestamp) {
        int i = index.getAndIncrement() % clockTimestamps.length;
        clockTimestamps[i] = clockTimestamp;
        activityTimestamps[i] = activityTimestamp;
    }

    public Sip suggest(Sip initialSip) {
        int lastIndex = index.get() - 1;
        if (lastIndex < 0) {
            return initialSip;
        }

        long latestTimestamp = clockTimestamps[lastIndex % clockTimestamps.length];
        long latestMinusSkew = latestTimestamp - maxSipClockSkew;
        if (lastIndex < clockTimestamps.length) {
            // fewer than the max replay size, so sip to the more distant timestamp
            if (clockTimestamps[0] < latestMinusSkew) {
                return new Sip(clockTimestamps[0], activityTimestamps[0]);
            } else {
                return new Sip(latestMinusSkew, 0);
            }
        } else {
            // more than the max replay size, so sip to the more recent timestamp
            int oldestIndex = index.get() % clockTimestamps.length;
            if (clockTimestamps[oldestIndex] > latestMinusSkew) {
                return new Sip(clockTimestamps[oldestIndex], activityTimestamps[oldestIndex]);
            } else {
                return new Sip(latestMinusSkew, 0);
            }
        }
    }

    public boolean wasSeenLastSip(TimeAndVersion timeAndVersion) {
        return seenLastSip.contains(timeAndVersion);
    }

    public void addSeenThisSip(TimeAndVersion timeAndVersion) {
        seenThisSip.add(timeAndVersion);
    }

}
