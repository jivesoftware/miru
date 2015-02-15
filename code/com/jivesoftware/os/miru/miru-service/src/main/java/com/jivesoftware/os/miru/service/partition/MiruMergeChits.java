package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class MiruMergeChits {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();


    private final long targetChits;
    private final double rateOfConvergence;
    private final long convergenceInterval;
    private final AtomicLong numberOfChitsRemaining;

    private final AtomicLong maxElapsedWithoutMergeInMillis = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong lastConvergenceTime = new AtomicLong(Long.MAX_VALUE);
    private final AtomicBoolean targetSeen = new AtomicBoolean(false);
    private final AtomicLong highestElapsed = new AtomicLong(0);

    public MiruMergeChits(long maxChits, double rateOfConvergence, long convergenceInterval) {
        this.targetChits = (long) (maxChits * 0.5);
        this.numberOfChitsRemaining = new AtomicLong(maxChits);
        this.rateOfConvergence = rateOfConvergence;
        this.convergenceInterval = convergenceInterval;
    }

    public void take(long count) {
        long chitsFree = numberOfChitsRemaining.addAndGet(-count);
        log.set(ValueType.COUNT, "chit>free", chitsFree);
    }

    public void refund(long count) {
        long chitsFree = numberOfChitsRemaining.addAndGet(count);
        log.set(ValueType.COUNT, "chit>free", chitsFree);
    }

    public boolean merge(long indexed, long elapsed) {
        long chitsFree = numberOfChitsRemaining.get();
        long currentTime = System.currentTimeMillis();
        if (!targetSeen.get()) {
            long highest = highestElapsed.get();
            while (elapsed > highest) {
                if (highestElapsed.compareAndSet(highest, elapsed)) {
                    break;
                } else {
                    highest = highestElapsed.get();
                }
            }

            if (chitsFree < targetChits) {
                synchronized (lastConvergenceTime) {
                    if (targetSeen.compareAndSet(false, true)) {
                        maxElapsedWithoutMergeInMillis.set(highest);
                        lastConvergenceTime.set(currentTime);
                    }
                }
            }
        }

        synchronized (lastConvergenceTime) {
            long millisSinceLastConvergence = currentTime - lastConvergenceTime.get();
            if (millisSinceLastConvergence > 0) {
                // max = 1000
                // target = 500
                // free = 250  -> delta = (250-500)/500 = -0.5
                // free = 750  -> delta = (750-500)/500 = 0.5
                // free = 0    -> delta = -1.0
                // free = 1000 -> delta = 1.0
                double delta = (double) (chitsFree - targetChits) / (double) targetChits;
                double attenuatedDelta = delta * (double) millisSinceLastConvergence / (double) convergenceInterval;

                // maxElapse = 60,000 ms
                // maxChits = 1000
                // cost = 60 ms
                //
                // free = 250  -> 45,000 ms
                // free = 750  -> 75,000 ms
                // free = 0    -> 30,000 ms
                // free = 1000 -> 90,000 ms
                long skew = (long) (attenuatedDelta * maxElapsedWithoutMergeInMillis.get() * rateOfConvergence);

                long maxElapsed = maxElapsedWithoutMergeInMillis.addAndGet(skew);
                lastConvergenceTime.set(currentTime);
                log.set(ValueType.VALUE, "chit>maxElapsed", maxElapsed);
                log.set(ValueType.VALUE, "chit>lastConvergence", currentTime);
            }
        }

        boolean merge = elapsed > maxElapsedWithoutMergeInMillis.get();
        if (merge) {
            log.inc("chit>merged>total");
            log.inc("chit>merged>power>" + FilerIO.chunkPower(indexed, 0));
        }
        return merge;
    }

    public long numberOfChitsRemaining() {
        return numberOfChitsRemaining.get();
    }

    public long maxElapsedWithoutMergeInMillis() {
        return maxElapsedWithoutMergeInMillis.get();
    }
}
