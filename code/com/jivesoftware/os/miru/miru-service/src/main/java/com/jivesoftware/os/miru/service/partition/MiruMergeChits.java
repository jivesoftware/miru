package com.jivesoftware.os.miru.service.partition;

import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan.colt
 */
public class MiruMergeChits {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final long targetChits;
    private final double rateOfConvergence;
    private final AtomicLong numberOfChitsRemaining;
    private final AtomicLong maxElapseWithoutMergeInMillis = new AtomicLong(TimeUnit.MINUTES.toMillis(1));

    public MiruMergeChits(long maxChits, double rateOfConvergence) {
        this.targetChits = (long) (maxChits * 0.5);
        this.numberOfChitsRemaining = new AtomicLong(maxChits);
        this.rateOfConvergence = rateOfConvergence;
    }

    public boolean merge(long indexed, long elapse) {
        long maxElapse = maxElapseWithoutMergeInMillis.get();
        long chitsFree = numberOfChitsRemaining.get();

        // max = 1000
        // target = 500
        // free = 250  -> delta = (250-500)/500 = -0.5
        // free = 750  -> delta = (750-500)/500 = 0.5
        // free = 0    -> delta = -1.0
        // free = 1000 -> delta = 1.0
        double deltaAsPercent = (chitsFree - targetChits) / targetChits;

        // maxElapse = 60,000 ms
        // maxChits = 1000
        // cost = 60 ms
        //
        // free = 250  -> 45,000 ms
        // free = 750  -> 75,000 ms
        // free = 0    -> 30,000 ms
        // free = 1000 -> 90,000 ms
        maxElapse = maxElapseWithoutMergeInMillis.addAndGet((long) (deltaAsPercent * maxElapse * rateOfConvergence));
        log.set(ValueType.VALUE, "chit>maxElapse", maxElapse);

        boolean merge = elapse > maxElapse;
        if (merge) {
            log.inc("chit>merged>total");
            log.inc("chit>merged>power>" + FilerIO.chunkPower(indexed, 0));
        }
        return merge;
    }

    public void take(int count) {
        long chitsFree = numberOfChitsRemaining.addAndGet(-count);
        log.set(ValueType.COUNT, "chit>free", chitsFree);
    }

    public void refund(long count) {
        long chitsFree = numberOfChitsRemaining.addAndGet(count);
        log.set(ValueType.COUNT, "chit>free", chitsFree);
    }
}
