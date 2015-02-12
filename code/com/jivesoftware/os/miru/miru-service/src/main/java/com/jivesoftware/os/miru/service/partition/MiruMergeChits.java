package com.jivesoftware.os.miru.service.partition;

import com.google.common.util.concurrent.AtomicDouble;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan.colt
 */
public class MiruMergeChits {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final AtomicLong chits;
    private final AtomicDouble movingAvgOfMillisPerIndexed = new AtomicDouble(0);
    private final double mergeRateRatio;
    private final long maxElapseWithoutMergeInMillis;
    private final Set<MiruPartitionCoord> activeCoords = Collections.newSetFromMap(new ConcurrentHashMap<MiruPartitionCoord, Boolean>());

    public MiruMergeChits(long free, double mergeRateRatio, long maxElapseWithoutMergeInMillis) {
        chits = new AtomicLong(free);
        this.mergeRateRatio = mergeRateRatio;
        this.maxElapseWithoutMergeInMillis = maxElapseWithoutMergeInMillis;
    }

    public void take(int count) {
        long chitsFree = chits.addAndGet(-count);
        log.set(ValueType.COUNT, "chit>free", chitsFree);
    }

    public boolean merge(MiruPartitionCoord coord, long indexed, long elapse) {
        long hysteresis = (long) (maxElapseWithoutMergeInMillis * new Random(coord.hashCode()).nextDouble());
        if (elapse > maxElapseWithoutMergeInMillis + hysteresis) {
            log.inc("chits>merged>force>maxElapse");
            return true;
        }
        if (indexed <= 0) {
            return false;
        }
        if (elapse < 0) {
            elapse = 0;
        }
        double millisPerIndexed = (double) elapse / (double) indexed;
        double weight = 1d - (1d / activeCoords.size());
        movingAvgOfMillisPerIndexed.set(((movingAvgOfMillisPerIndexed.get() * weight) + (millisPerIndexed * (1d - weight))));

        double movingAvg = movingAvgOfMillisPerIndexed.get();
        log.set(ValueType.VALUE, "chit>millisPerIndex", (long) movingAvg);
        double scalar = 1;
        if (movingAvg > 0) {
            scalar += (millisPerIndexed / movingAvg) * (mergeRateRatio * 2); // * 2 magic inverse of div by 2 moving avg above.
        }

        long chitsFree = chits.get();
        boolean merge = (indexed * scalar) > chitsFree;
        if (merge) {
            log.inc("chit>merged>total");
            log.inc("chit>merged>power>" + FilerIO.chunkPower(indexed, 0));
        }
        return merge;
    }

    public void refund(long count) {
        chits.addAndGet(count);
    }

    void active(MiruPartitionCoord coord) {
        activeCoords.add(coord);
        log.set(ValueType.VALUE, "chit>activePartitions", activeCoords.size());
    }

    void inactive(MiruPartitionCoord coord) {
        activeCoords.remove(coord);
        log.set(ValueType.VALUE, "chit>activePartitions", activeCoords.size());
    }
}
