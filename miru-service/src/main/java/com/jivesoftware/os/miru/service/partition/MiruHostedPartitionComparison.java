package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.partition.MiruRoutablePartition;
import com.jivesoftware.os.miru.plugin.solution.MiruSolution;
import com.jivesoftware.os.miru.service.partition.cluster.PartitionAndHost;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collection;
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

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ConcurrentMap<TenantPartitionAndQuery, ConcurrentSkipListMap<PartitionAndHost, Long>> coordRecency = Maps.newConcurrentMap();
    private final ConcurrentMap<TenantPartitionAndQuery, RunningPercentile> queryPercentile = Maps.newConcurrentMap();

    private final int windowSize;
    private final int percentile;
    private final Timestamper timestamper;

    private final Comparator<PartitionAndTime> partitionAndTimeComparator = (pat1, pat2) -> {
        MiruRoutablePartition p1 = pat1.partition;
        MiruRoutablePartition p2 = pat2.partition;
        long t1 = pat1.time;
        long t2 = pat2.time;
        return ComparisonChain
            .start()
            .compare(p2.partitionId, p1.partitionId) // flipped p1 and p2 so that we get descending order.
            .compare(t2, t1) // descending order
            .compare(p1.host, p2.host)
            .result();
    };

    public MiruHostedPartitionComparison(int windowSize, int percentile, Timestamper timestamper) {
        this.windowSize = windowSize;
        this.percentile = percentile;
        this.timestamper = timestamper;
    }

    public MiruHostedPartitionComparison(int percentile, int windowSize) {
        this(percentile, windowSize, System::currentTimeMillis);
    }

    /**
     * Orders partitions for a replica set based on query metrics.
     *
     * @param tenantId    the tenant
     * @param partitionId the partition
     * @param requestName the request name
     * @param queryKey    the query key
     * @param partitions  the partitions to order   @return a stable comparator
     */
    public List<MiruRoutablePartition> orderPartitions(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        String requestName,
        String queryKey,
        Collection<MiruRoutablePartition> partitions) {

        ConcurrentSkipListMap<PartitionAndHost, Long> skipList = coordRecency.get(new TenantPartitionAndQuery(tenantId, partitionId, requestName, queryKey));
        Iterator<Map.Entry<PartitionAndHost, Long>> skipIter = skipList != null
            ? skipList.entrySet().iterator() : Iterators.emptyIterator();
        Map.Entry<PartitionAndHost, Long> skipEntry = skipIter.hasNext() ? skipIter.next() : null;

        List<PartitionAndTime> partitionAndTimes = Lists.newArrayListWithCapacity(partitions.size());

        for (MiruRoutablePartition partition : partitions) {
            if (partition.destroyAfterTimestamp > 0 && System.currentTimeMillis() > partition.destroyAfterTimestamp) {
                continue;
            }

            PartitionAndHost partitionAndHost = new PartitionAndHost(partition.partitionId, partition.host);
            while (skipEntry != null && skipEntry.getKey().compareTo(partitionAndHost) < 0) {
                skipEntry = skipIter.hasNext() ? skipIter.next() : null;
            }
            long time = Long.MIN_VALUE;
            if (skipEntry != null && skipEntry.getKey().compareTo(partitionAndHost) == 0) {
                time = skipEntry.getValue();
            }
            partitionAndTimes.add(new PartitionAndTime(partition, time));
        }

        partitionAndTimes.sort(partitionAndTimeComparator);
        return Lists.transform(partitionAndTimes, input -> input.partition);
    }

    /**
     * Analyzes the latest winning solutions for a replica set.
     *
     * @param solutions  the latest winning solutions
     * @param queryKey   the query key
     */
    public void analyzeSolutions(List<MiruSolution> solutions, String requestName, String queryKey) {
        for (MiruSolution solution : solutions) {
            MiruPartitionCoord coord = solution.usedPartition;
            TenantPartitionAndQuery key = new TenantPartitionAndQuery(coord.tenantId, coord.partitionId, requestName, queryKey);
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

            LOG.debug("Solution for {} {}: {}", requestName, queryKey, solution);
            runningPercentile.add(solution.usedResultElapsed);
        }
    }

    public Optional<Long> suggestTimeout(MiruTenantId tenantId, MiruPartitionId partitionId, String requestName, String queryKey) {
        RunningPercentile runningPercentile = queryPercentile.get(new TenantPartitionAndQuery(tenantId, partitionId, requestName, queryKey));
        if (runningPercentile != null) {
            long suggestion = runningPercentile.get();
            if (suggestion > 0) {
                LOG.debug("Suggested {} for {} {} {}", suggestion, tenantId, partitionId, queryKey);
                return Optional.of(suggestion);
            }
        }
        return Optional.absent();
    }

    private static class TenantPartitionAndQuery {

        private final MiruTenantId tenantId;
        private final MiruPartitionId partitionId;
        private final String requestName;
        private final String queryKey;

        private TenantPartitionAndQuery(MiruTenantId tenantId, MiruPartitionId partitionId, String requestName, String queryKey) {
            this.tenantId = tenantId;
            this.partitionId = partitionId;
            this.requestName = requestName;
            this.queryKey = queryKey;
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

            if (tenantId != null ? !tenantId.equals(that.tenantId) : that.tenantId != null) {
                return false;
            }
            if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null) {
                return false;
            }
            if (requestName != null ? !requestName.equals(that.requestName) : that.requestName != null) {
                return false;
            }
            return !(queryKey != null ? !queryKey.equals(that.queryKey) : that.queryKey != null);

        }

        @Override
        public int hashCode() {
            int result = tenantId != null ? tenantId.hashCode() : 0;
            result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
            result = 31 * result + (requestName != null ? requestName.hashCode() : 0);
            result = 31 * result + (queryKey != null ? queryKey.hashCode() : 0);
            return result;
        }
    }

    public interface Timestamper {
        long get();
    }

    private static class PartitionAndTime {

        public final MiruRoutablePartition partition;
        public final long time;

        private PartitionAndTime(MiruRoutablePartition partition, long time) {
            this.partition = partition;
            this.time = time;
        }
    }

}
