package com.jivesoftware.os.miru.service.partition;

import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class FreeMergeChits implements MiruMergeChits {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final String name;

    private final AtomicLong numberOfChitsTaken = new AtomicLong();
    private final Map<MiruPartitionCoord, AtomicLong> chits = Maps.newConcurrentMap();

    public FreeMergeChits(String name) {
        this.name = name;
    }

    @Override
    public void refundAll(MiruPartitionCoord coord) {
        AtomicLong taken = chits.remove(coord);
        if (taken != null) {
            long chitsTaken = numberOfChitsTaken.addAndGet(-taken.get());
            log.set(ValueType.COUNT, "chit>" + name + ">taken", chitsTaken);
        }
    }

    @Override
    public boolean take(MiruPartitionCoord coord, long count) {
        AtomicLong taken = chits.computeIfAbsent(coord, (key) -> new AtomicLong());
        taken.addAndGet(count);
        long chitsTaken = numberOfChitsTaken.addAndGet(count);
        log.set(ValueType.COUNT, "chit>" + name + ">taken", chitsTaken);
        return false;
    }

    @Override
    public long taken(MiruPartitionCoord coord) {
        AtomicLong taken = chits.get(coord);
        return taken == null ? 0 : taken.get();
    }

    @Override
    public long remaining() {
        return Long.MAX_VALUE;
    }
}
