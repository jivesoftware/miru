package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.service.stream.factory.ExecuteQuery;
import com.jivesoftware.os.miru.service.stream.factory.MiruSolution;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class MiruHostedPartitionComparison {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final ConcurrentMap<MiruPartitionCoord, Long> coordRecency = Maps.newConcurrentMap();
    private final ConcurrentMap<TenantPartitionAndQuery, RunningPercentile> queryPercentile = Maps.newConcurrentMap();

    private final int windowSize;
    private final int percentile;
    private final Timestamper timestamper;

    public MiruHostedPartitionComparison(int windowSize, int percentile, Timestamper timestamper) {
        this.windowSize = windowSize;
        this.percentile = percentile;
        this.timestamper = timestamper;
    }

    public MiruHostedPartitionComparison(int percentile, int windowSize) {
        this(percentile, windowSize, new Timestamper() {
            @Override
            public long get() {
                return System.currentTimeMillis();
            }
        });
    }

    /**
     * Turns current metrics into a stable comparator.
     *
     * @return a stable comparator
     */
    public Comparator<MiruHostedPartition> getComparator() {
        final Map<MiruPartitionCoord, Long> stableRecency = ImmutableMap.copyOf(coordRecency);
        return new Comparator<MiruHostedPartition>() {
            @Override
            public int compare(MiruHostedPartition p1, MiruHostedPartition p2) {
                long t1 = getRecencyTime(p1);
                long t2 = getRecencyTime(p2);
                return ComparisonChain
                    .start()
                    .compare(p2.getPartitionId(), p1.getPartitionId()) // flipped p1 and p2 so that we get descending order.
                    .compareTrueFirst(p1.isLocal(), p2.isLocal())
                    .compare(p1.getStorage(), p2.getStorage(), Ordering.explicit(
                        MiruBackingStorage.memory_fixed,
                        MiruBackingStorage.memory,
                        MiruBackingStorage.hybrid_fixed,
                        MiruBackingStorage.hybrid,
                        MiruBackingStorage.mem_mapped,
                        MiruBackingStorage.disk,
                        MiruBackingStorage.unknown))
                    .compare(t2, t1) // descending order
                    .result();
            }

            private long getRecencyTime(MiruHostedPartition p) {
                Long t = stableRecency.get(p.getCoord());
                if (t != null) {
                    return t;
                }
                return Long.MIN_VALUE;
            }
        };
    }

    /**
     * Analyzes the latest winning solutions for a replica set.
     *
     * @param solutions the latest winning solutions
     */
    public <R> void analyzeSolutions(List<MiruSolution<R>> solutions) {
        for (MiruSolution<R> solution : solutions) {
            MiruPartitionCoord coord = solution.getCoord();
            coordRecency.put(coord, timestamper.get());

            TenantPartitionAndQuery key = new TenantPartitionAndQuery(coord.tenantId, coord.partitionId, solution.getQueryClass());
            RunningPercentile runningPercentile = queryPercentile.get(key);
            if (runningPercentile == null) {
                queryPercentile.putIfAbsent(key, new RunningPercentile(windowSize, percentile));
                runningPercentile = queryPercentile.get(key);
            }
            runningPercentile.add(solution.getElapsed());
        }
    }

    public <T extends ExecuteQuery<?, ?>> Optional<Long> suggestTimeout(MiruTenantId tenantId, MiruPartitionId partitionId, Class<T> queryClass) {
        RunningPercentile runningPercentile = queryPercentile.get(new TenantPartitionAndQuery(tenantId, partitionId, queryClass));
        if (runningPercentile != null) {
            long suggestion = runningPercentile.get();
            if (suggestion > 0) {
                log.debug("Suggested {} for {} {} {}", suggestion, tenantId, partitionId, queryClass.getSimpleName());
                return Optional.of(suggestion);
            }
        }
        return Optional.absent();
    }

    private static class TenantPartitionAndQuery {

        private final MiruTenantId tenantId;
        private final MiruPartitionId partitionId;
        private final Class<? extends ExecuteQuery<?, ?>> queryClass;

        private TenantPartitionAndQuery(MiruTenantId tenantId, MiruPartitionId partitionId, Class<? extends ExecuteQuery<?, ?>> queryClass) {
            this.tenantId = tenantId;
            this.partitionId = partitionId;
            this.queryClass = queryClass;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TenantPartitionAndQuery that = (TenantPartitionAndQuery) o;

            if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null) {
                return false;
            }
            if (queryClass != null ? !queryClass.equals(that.queryClass) : that.queryClass != null) {
                return false;
            }
            if (tenantId != null ? !tenantId.equals(that.tenantId) : that.tenantId != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = tenantId != null ? tenantId.hashCode() : 0;
            result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
            result = 31 * result + (queryClass != null ? queryClass.hashCode() : 0);
            return result;
        }
    }

    public static interface Timestamper {
        long get();
    }

}
