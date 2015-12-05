package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class MiruMergeChits {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final AtomicLong numberOfChitsRemaining;
    private final long maxOverage;
    private final StripingLocksProvider<MiruPartitionCoord> stripingLocks = new StripingLocksProvider<>(128);
    private final Map<MiruPartitionCoord, AtomicLong> mergeQueue = Collections.synchronizedMap(Maps.<MiruPartitionCoord, AtomicLong>newLinkedHashMap());

    public MiruMergeChits(AtomicLong numberOfChitsRemaining, long maxChits, long maxOverage) {
        this.numberOfChitsRemaining = numberOfChitsRemaining;
        this.maxOverage = maxOverage >= 0 ? maxOverage : maxChits;
    }

    public void refundAll(MiruPartitionCoord coord) {
        AtomicLong taken = mergeQueue.remove(coord);
        if (taken != null) {
            long chitsFree = numberOfChitsRemaining.addAndGet(taken.get());
            log.set(ValueType.COUNT, "chit>free", chitsFree);
        }
    }

    public boolean take(MiruPartitionCoord coord, long count) {
        long chitsFree = numberOfChitsRemaining.addAndGet(-count);
        AtomicLong taken = mergeQueue.get(coord);
        if (taken == null) {
            synchronized (stripingLocks.lock(coord, 0)) {
                taken = mergeQueue.get(coord);
                if (taken == null) {
                    taken = new AtomicLong(0);
                    mergeQueue.put(coord, taken);
                }
            }
        }
        taken.addAndGet(count);
        log.set(ValueType.COUNT, "chit>free", chitsFree);

        return canMerge(coord);
    }

    public long taken(MiruPartitionCoord coord) {
        AtomicLong taken = mergeQueue.get(coord);
        return taken != null ? taken.get() : -1;
    }

    private boolean canMerge(MiruPartitionCoord coord) {
        long chitsFree = numberOfChitsRemaining.get();

        if (chitsFree >= 0) {
            return false;
        }

        AtomicLong taken = mergeQueue.get(coord);
        if (taken == null || taken.get() <= 0) {
            return true;
        }

        long overage = Math.abs(chitsFree);
        synchronized (mergeQueue) {
            for (Map.Entry<MiruPartitionCoord, AtomicLong> entry : mergeQueue.entrySet()) {
                long got = entry.getValue().get();
                long requiredOverage = Math.min(maxOverage, got / 2);
                if (entry.getKey().equals(coord)) {
                    if (overage >= requiredOverage) {
                        log.inc("chit>merged>total");
                        log.inc("chit>merged>power>" + FilerIO.chunkPower(got, 0));
                        return true;
                    } else {
                        break;
                    }
                } else {
                    overage -= requiredOverage;
                }
                if (overage <= 0) {
                    break;
                }
            }
        }
        return false;
    }

    public long remaining() {
        return numberOfChitsRemaining.get();
    }
}
