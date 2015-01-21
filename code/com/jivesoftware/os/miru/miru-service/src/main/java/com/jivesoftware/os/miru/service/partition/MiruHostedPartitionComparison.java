package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.plugin.solution.MiruSolution;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 *
 */
public class MiruHostedPartitionComparison {

    private static final MetricLogger log = MetricLoggerFactory.getLogger();

    private final ConcurrentMap<TenantPartitionAndQuery, ConcurrentSkipListMap<PartitionAndHost, Long>> coordRecency = Maps.newConcurrentMap();
    private final ConcurrentMap<TenantPartitionAndQuery, RunningPercentile> queryPercentile = Maps.newConcurrentMap();

    private final int windowSize;
    private final int percentile;
    private final Timestamper timestamper;

    private final Comparator<PartitionAndTime<?>> partitionAndTimeComparator = new Comparator<PartitionAndTime<?>>() {
        @Override
        public int compare(PartitionAndTime<?> pat1, PartitionAndTime<?> pat2) {
            MiruHostedPartition p1 = pat1.partition;
            MiruHostedPartition p2 = pat2.partition;
            long t1 = pat1.time;
            long t2 = pat2.time;
            return ComparisonChain
                .start()
                .compare(p2.getPartitionId(), p1.getPartitionId()) // flipped p1 and p2 so that we get descending order.
                .compareTrueFirst(p1.isLocal(), p2.isLocal())
                .compare(p1.getStorage(), p2.getStorage(), Ordering.explicit(
                    MiruBackingStorage.hybrid,
                    MiruBackingStorage.mem_mapped,
                    MiruBackingStorage.unknown))
                .compare(t2, t1) // descending order
                .result();
        }
    };

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
     * Orders partitions for a replica set based on query metrics.
     *
     * @param tenantId    the tenant
     * @param partitionId the partition
     * @param queryKey    the query key
     * @param partitions  the partitions to order
     * @return a stable comparator
     */
    public <BM> List<MiruHostedPartition<BM>> orderPartitions(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        String queryKey,
        Collection<MiruHostedPartition<BM>> partitions) {

        ConcurrentSkipListMap<PartitionAndHost, Long> skipList = coordRecency.get(new TenantPartitionAndQuery(tenantId, partitionId, queryKey));
        Iterator<Map.Entry<PartitionAndHost, Long>> skipIter = skipList != null
            ? skipList.entrySet().iterator() : Iterators.<Map.Entry<PartitionAndHost, Long>>emptyIterator();
        Map.Entry<PartitionAndHost, Long> skipEntry = skipIter.hasNext() ? skipIter.next() : null;

        List<PartitionAndTime<BM>> partitionAndTimes = Lists.newArrayListWithCapacity(partitions.size());

        for (MiruHostedPartition<BM> partition : partitions) {
            PartitionAndHost partitionAndHost = new PartitionAndHost(partition.getCoord().partitionId, partition.getCoord().host);
            while (skipEntry != null && skipEntry.getKey().compareTo(partitionAndHost) < 0) {
                skipEntry = skipIter.hasNext() ? skipIter.next() : null;
            }
            long time = Long.MIN_VALUE;
            if (skipEntry != null && skipEntry.getKey().compareTo(partitionAndHost) == 0) {
                time = skipEntry.getValue();
            }
            partitionAndTimes.add(new PartitionAndTime<>(partition, time));
        }

        Collections.sort(partitionAndTimes, partitionAndTimeComparator);
        return Lists.transform(partitionAndTimes, new Function<PartitionAndTime<BM>, MiruHostedPartition<BM>>() {
            @Override
            public MiruHostedPartition<BM> apply(PartitionAndTime<BM> input) {
                return input.partition;
            }
        });
    }

    /**
     * Analyzes the latest winning solutions for a replica set.
     *
     * @param solutions  the latest winning solutions
     * @param queryClass the query class
     */
    public void analyzeSolutions(List<MiruSolution> solutions, String queryClass) {
        for (MiruSolution solution : solutions) {
            MiruPartitionCoord coord = solution.usedPartition;
            TenantPartitionAndQuery key = new TenantPartitionAndQuery(coord.tenantId, coord.partitionId, queryClass);
            ConcurrentSkipListMap<PartitionAndHost, Long> skipList = coordRecency.get(key);
            if (skipList == null) {
                skipList = new ConcurrentSkipListMap<>();
                ConcurrentSkipListMap<PartitionAndHost, Long> existing = coordRecency.putIfAbsent(key, skipList);
                if (existing != null) {
                    skipList = existing;
                }
            }
            skipList.put(new PartitionAndHost(coord.partitionId, coord.host), timestamper.get());

            RunningPercentile runningPercentile = queryPercentile.get(key);
            if (runningPercentile == null) {
                queryPercentile.putIfAbsent(key, new RunningPercentile(windowSize, percentile));
                runningPercentile = queryPercentile.get(key);
            }
            runningPercentile.add(solution.usedResultElapsed);
        }
    }

    public Optional<Long> suggestTimeout(MiruTenantId tenantId, MiruPartitionId partitionId, String queryClass) {
        RunningPercentile runningPercentile = queryPercentile.get(new TenantPartitionAndQuery(tenantId, partitionId, queryClass));
        if (runningPercentile != null) {
            long suggestion = runningPercentile.get();
            if (suggestion > 0) {
                log.debug("Suggested {} for {} {} {}", suggestion, tenantId, partitionId, queryClass);
                return Optional.of(suggestion);
            }
        }
        return Optional.absent();
    }

    private static class TenantPartitionAndQuery {

        private final MiruTenantId tenantId;
        private final MiruPartitionId partitionId;
        private final String queryClass;

        private TenantPartitionAndQuery(MiruTenantId tenantId, MiruPartitionId partitionId, String queryClass) {
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
            return !(tenantId != null ? !tenantId.equals(that.tenantId) : that.tenantId != null);
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

    private static class PartitionAndTime<BM> {

        public final MiruHostedPartition<BM> partition;
        public final long time;

        private PartitionAndTime(MiruHostedPartition<BM> partition, long time) {
            this.partition = partition;
            this.time = time;
        }
    }

}
