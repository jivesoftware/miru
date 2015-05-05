package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.Multiset;
import com.google.common.collect.SortedMultiset;
import com.google.common.collect.TreeMultiset;
import java.util.concurrent.atomic.AtomicLong;

/**
*
*/
public class RunningPercentile {

    private final int windowSize;
    private final int percentile;
    private final SortedMultiset<Long> lowerTimes;
    private final SortedMultiset<Long> upperTimes;
    private final AtomicLong position = new AtomicLong();
    private final long[] times;

    public RunningPercentile(int windowSize, int percentile) {
        this.windowSize = windowSize;
        this.percentile = percentile;
        this.lowerTimes = TreeMultiset.create();
        this.upperTimes = TreeMultiset.create();
        this.times = new long[windowSize];
    }

    public void add(long time) {
        if (time < 0) {
            return;
        }
        synchronized (position) {
            long currentPosition = position.getAndIncrement();

            int totalSize;
            if (currentPosition >= windowSize) {
                totalSize = windowSize;

                // remove oldest value in running window
                long oldTime = times[(int) (currentPosition % windowSize)];
                if (!lowerTimes.remove(oldTime)) {
                    if (!upperTimes.remove(oldTime)) {
                        throw new IllegalStateException("Old value did not exist in time sets");
                    }
                }
            } else {
                totalSize = lowerTimes.size() + upperTimes.size() + 1;
            }

            int lowerSize = Math.max(totalSize * percentile / 100, 1);

            if (lowerTimes.size() < lowerSize) {
                lowerTimes.add(time);
            } else {
                upperTimes.add(time);
            }

            while (true) {
                Multiset.Entry<Long> lowerLast = lowerTimes.lastEntry();
                Multiset.Entry<Long> upperFirst = upperTimes.firstEntry();

                if (lowerLast == null || upperFirst == null || lowerLast.getElement() <= upperFirst.getElement()) {
                    break;
                }

                lowerTimes.remove(lowerLast.getElement());
                lowerTimes.add(upperFirst.getElement());

                upperTimes.remove(upperFirst.getElement());
                upperTimes.add(lowerLast.getElement());
            }

            times[(int) (currentPosition % windowSize)] = time;
        }
    }

    public long get() {
        if (lowerTimes.isEmpty() && upperTimes.isEmpty()) {
            return -1;
        } else if (upperTimes.isEmpty()) {
            return lowerTimes.lastEntry().getElement();
        } else {
            return (lowerTimes.lastEntry().getElement() + upperTimes.firstEntry().getElement()) / 2;
        }
    }
}
