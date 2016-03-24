package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class LargestFirstMergeChits implements MiruMergeChits {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String name;
    private final AtomicLong numberOfChitsRemaining;
    private final StripingLocksProvider<MiruPartitionCoord> stripingLocks = new StripingLocksProvider<>(128);
    private final NavigableSet<Chitty> mergeQueue = Sets.newTreeSet();
    private final Map<MiruPartitionCoord, Chitty> chittiness = Maps.newConcurrentMap();

    public LargestFirstMergeChits(String name, AtomicLong numberOfChitsRemaining) {
        this.name = name;
        this.numberOfChitsRemaining = numberOfChitsRemaining;
    }

    @Override
    public void refundAll(MiruPartitionCoord coord) {
        synchronized (stripingLocks.lock(coord, 0)) {
            Chitty chitty = chittiness.get(coord);
            if (chitty != null) {
                boolean removed;
                synchronized (mergeQueue) {
                    removed = mergeQueue.remove(chitty);
                }
                if (removed) {
                    long chitsFree = numberOfChitsRemaining.addAndGet(chitty.taken);
                    chitty.taken = 0;
                    log.set(ValueType.COUNT, "chit>" + name + ">free", chitsFree);
                }
            }
        }
    }

    @Override
    public boolean take(MiruPartitionCoord coord, long count) {
        synchronized (stripingLocks.lock(coord, 0)) {
            Chitty chitty = chittiness.computeIfAbsent(coord, key -> new Chitty(coord, 0));
            long chitsFree = numberOfChitsRemaining.addAndGet(-count);
            synchronized (mergeQueue) {
                mergeQueue.remove(chitty);
                chitty.taken += count;
                mergeQueue.add(chitty);
            }
            log.set(ValueType.COUNT, "chit>" + name + ">free", chitsFree);
        }

        return canMerge(coord);
    }

    @Override
    public long taken(MiruPartitionCoord coord) {
        Chitty chitty = chittiness.get(coord);
        return chitty != null ? chitty.taken : 0;
    }

    private boolean canMerge(MiruPartitionCoord coord) {
        long chitsFree = numberOfChitsRemaining.get();

        if (chitsFree >= 0) {
            return false;
        }

        Chitty chitty = chittiness.get(coord);
        if (chitty == null || chitty.taken <= 0) {
            return false;
        }

        Set<MiruPartitionCoord> eligible = Sets.newHashSet();
        synchronized (mergeQueue) {
            int firstN = Math.max(mergeQueue.size() / 20, 1);
            Iterator<Chitty> iter = mergeQueue.iterator();
            while (eligible.size() < firstN && iter.hasNext()) {
                eligible.add(iter.next().coord);
            }
        }

        boolean canMerge = eligible.contains(chitty.coord);
        if (canMerge) {
            log.inc("chit>" + name + ">merged>total");
            log.inc("chit>" + name + ">merged>power>" + FilerIO.chunkPower(chitty.taken, 0));
        }
        return canMerge;
    }

    @Override
    public long remaining() {
        return numberOfChitsRemaining.get();
    }

    private static class Chitty implements Comparable<Chitty> {

        private final MiruPartitionCoord coord;
        private long taken;

        public Chitty(MiruPartitionCoord coord, long taken) {
            this.coord = coord;
            this.taken = taken;
        }

        @Override
        public int compareTo(Chitty o) {
            int c = Long.compare(o.taken, taken);
            if (c != 0) {
                return c;
            }
            c = coord.tenantId.compareTo(o.coord.tenantId);
            if (c != 0) {
                return c;
            }
            c = coord.partitionId.compareTo(o.coord.partitionId);
            if (c != 0) {
                return c;
            }
            return coord.host.compareTo(o.coord.host);
        }
    }
}
