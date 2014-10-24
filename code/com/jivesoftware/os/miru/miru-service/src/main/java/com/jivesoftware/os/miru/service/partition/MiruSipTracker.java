package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.Sets;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class MiruSipTracker {

    private final long maxSipClockSkew;

    private final long[] clockTimestamps;
    private final AtomicInteger index;

    private final Set<TimeAndVersion> seenLastSip;
    private final Set<TimeAndVersion> seenThisSip;

    public MiruSipTracker(int maxSipReplaySize, long maxSipClockSkew, Set<TimeAndVersion> seenLastSip) {
        this.maxSipClockSkew = maxSipClockSkew;

        this.clockTimestamps = new long[maxSipReplaySize];
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

    public void put(long clockTimestamp) {
        clockTimestamps[index.getAndIncrement() % clockTimestamps.length] = clockTimestamp;
    }

    public long suggestTimestamp(long initialTimestamp) {
        int lastIndex = index.get() - 1;
        if (lastIndex < 0) {
            return initialTimestamp;
        }

        long latestTimestamp = clockTimestamps[lastIndex % clockTimestamps.length];
        if (lastIndex < clockTimestamps.length) {
            // fewer than the max replay size, so sip to the more distant timestamp
            return Math.min(clockTimestamps[0], latestTimestamp - maxSipClockSkew);
        } else {
            // more than the max replay size, so sip to the more recent timestamp
            long oldestTimestamp = clockTimestamps[index.get() % clockTimestamps.length];
            return Math.max(oldestTimestamp + 1, latestTimestamp - maxSipClockSkew);
        }
    }

    public boolean wasSeenLastSip(TimeAndVersion timeAndVersion) {
        return seenLastSip.contains(timeAndVersion);
    }

    public void addSeenThisSip(TimeAndVersion timeAndVersion) {
        seenThisSip.add(timeAndVersion);
    }
}
