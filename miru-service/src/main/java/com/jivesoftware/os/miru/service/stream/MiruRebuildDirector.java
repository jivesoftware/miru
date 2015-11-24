package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class MiruRebuildDirector {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final AtomicLong activityCount;
    private final Set<MiruPartitionCoord> prioritized = Sets.newHashSet();

    public MiruRebuildDirector(long maxConcurrentActivityCount) {
        this.activityCount = new AtomicLong(maxConcurrentActivityCount);
    }

    public Optional<Token> acquire(MiruPartitionCoord coord, long count) {
        if (activityCount.get() >= count) {
            if (isPermitted(coord)) {
                synchronized (activityCount) {
                    if (activityCount.get() >= count) {
                        activityCount.addAndGet(-count);
                        if (prioritized.remove(coord)) {
                            LOG.info("Prioritized rebuild of {} has begun", coord);
                        }
                        return Optional.of(new Token(coord, count));
                    }
                }
            } else {
                LOG.debug("Refusing to rebuild {} due to prioritization: {}", coord, prioritized);
            }
        }
        return Optional.absent();
    }

    public void release(Token token) {
        activityCount.addAndGet(token.count);
        // prioritization should already have been removed on token acquire, but remove just in case priority was given while partition was rebuilding.
        prioritized.remove(token.coord);
    }

    public long available() {
        return activityCount.get();
    }

    public void prioritize(MiruLocalHostedPartition<?, ?, ?, ?> partition) {
        LOG.info("Prioritizing rebuild of {}", partition.getCoord());
        prioritized.add(partition.getCoord());
    }

    private boolean isPermitted(MiruPartitionCoord coord) {
        return prioritized.isEmpty() || prioritized.contains(coord);
    }

    public static class Token {
        private final MiruPartitionCoord coord;
        private final long count;

        private Token(MiruPartitionCoord coord, long count) {
            this.coord = coord;
            this.count = count;
        }
    }
}
