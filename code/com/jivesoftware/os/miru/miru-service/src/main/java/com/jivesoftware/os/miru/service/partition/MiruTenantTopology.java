package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.base.util.locks.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.plugin.partition.OrderedPartitions;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class MiruTenantTopology<BM> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruBitmaps<BM> bitmaps;
    private final MiruHost localHost;
    private final MiruTenantId tenantId;
    private final MiruLocalPartitionFactory localPartitionFactory;
    private final MiruRemotePartitionFactory remotePartitionFactory;
    private final ConcurrentMap<MiruPartitionCoord, MiruHostedPartition<BM>> topology;
    private final Cache<MiruPartitionCoord, Boolean> sticky;
    private final MiruHostedPartitionComparison partitionComparison;

    private final StripingLocksProvider<MiruPartitionCoord> topologyLock = new StripingLocksProvider<>(64);

    public MiruTenantTopology(
            MiruServiceConfig config,
            MiruBitmaps<BM> bitmaps,
            MiruHost localHost,
            MiruTenantId tenantId,
            MiruLocalPartitionFactory localPartitionFactory,
            MiruRemotePartitionFactory remotePartitionFactory,
            MiruHostedPartitionComparison partitionComparison) {
        this.bitmaps = bitmaps;
        this.localHost = localHost;
        this.tenantId = tenantId;
        this.localPartitionFactory = localPartitionFactory;
        this.remotePartitionFactory = remotePartitionFactory;
        this.topology = Maps.newConcurrentMap();
        this.sticky = CacheBuilder.newBuilder()
            .expireAfterWrite(config.getEnsurePartitionsIntervalInMillis() * 2, TimeUnit.MILLISECONDS) // double the ensurePartitions interval
            .build();
        this.partitionComparison = partitionComparison;
    }

    public MiruTenantId getTenantId() {
        return tenantId;
    }

    public void index(List<MiruPartitionedActivity> activities) throws Exception {
        List<MiruPartitionedActivity> mutableActivities = Lists.newLinkedList(activities);

        while (!mutableActivities.isEmpty()) {
            for (MiruHostedPartition partition : topology.values()) {
                try {
                    partition.index(mutableActivities.iterator());
                } catch (Exception x) {
                    LOG.error("Partition failed to index. partition:" + partition, x);
                }

                if (mutableActivities.isEmpty()) {
                    break;
                }
            }

            ensurePartitions(mutableActivities);
        }
    }

    public void warm() {
        for (MiruHostedPartition partition : topology.values()) {
            partition.warm();
        }
    }

    public void setStorageForHost(MiruPartitionId partitionId, MiruBackingStorage storage, MiruHost host) throws Exception {
        Optional<MiruHostedPartition<?>> partition = getPartition(new MiruPartitionCoord(tenantId, partitionId, host));
        if (partition.isPresent()) {
            partition.get().setStorage(storage);
        }
    }

    public void checkForPartitionAlignment(Collection<MiruPartitionCoord> coordsForTenantHost) throws Exception {
        Set<MiruPartitionCoord> expected = Sets.newHashSet(coordsForTenantHost);
        Set<MiruPartitionCoord> adding;
        Set<MiruPartitionCoord> removing;
        synchronized (topology) {
            Set<MiruPartitionCoord> knownCoords = topology.keySet();
            adding = Sets.newHashSet(expected);
            adding.removeAll(knownCoords);

            removing = Sets.newHashSet(knownCoords);
            removing.removeAll(expected);
            removing.removeAll(sticky.asMap().keySet());
        }

        for (MiruPartitionCoord add : adding) {
            ensureTopology(add);
        }

        for (MiruPartitionCoord remove : removing) {
            removeTopology(remove);
        }
    }

    public Iterable<OrderedPartitions> allPartitionsInOrder() {
        List<MiruHostedPartition<BM>> sortedPartitions = Lists.newArrayList(topology.values());
        Collections.sort(sortedPartitions, partitionComparison.getComparator());

        List<OrderedPartitions> allPartitions = Lists.newArrayList();
        List<MiruHostedPartition<?>> partitions = Lists.newArrayList();
        MiruHostedPartition<BM> lastPartition = null;
        for (MiruHostedPartition<BM> partition : sortedPartitions) {
            if (lastPartition == null) {
                lastPartition = partition;
                partitions.add(partition);
            } else {
                if (!lastPartition.getPartitionId().equals(partition.getPartitionId())) {
                    if (!partitions.isEmpty()) {
                        allPartitions.add(new OrderedPartitions(tenantId, lastPartition.getPartitionId(), partitions));
                    }
                    partitions = Lists.newArrayList();
                }
                partitions.add(partition);
                lastPartition = partition;
            }
        }
        if (!partitions.isEmpty() && lastPartition != null) {
            allPartitions.add(new OrderedPartitions(tenantId, lastPartition.getPartitionId(), partitions));
        }
        return allPartitions;
    }

    public Optional<MiruHostedPartition<?>> getPartition(MiruPartitionCoord miruPartitionCoord) {
        MiruHostedPartition<?> partition = topology.get(miruPartitionCoord);
        return Optional.<MiruHostedPartition<?>>fromNullable(partition);
    }

    public void remove() {
        for (MiruHostedPartition partition : topology.values()) {
            try {
                partition.remove();
            } catch (Exception x) {
                LOG.error("Partition failed to remove. partition:" + partition, x);
            }
        }
        topology.clear();
    }

    /**
     * A writer thinks we need these activities. Chances are there was a partition rollover.
     * Bootstrap on the fly and make sticky while expected tenancy shakes out.
     */
    private void ensurePartitions(List<MiruPartitionedActivity> activities) throws Exception {
        Set<MiruPartitionId> ensured = Sets.newHashSet();
        Iterator<MiruPartitionedActivity> iter = activities.iterator();
        while (iter.hasNext()) {
            MiruPartitionedActivity activity = iter.next();
            if (!activity.type.isActivityType()) {
                // only activity types can bootstrap
                iter.remove();
            } else if (!ensured.contains(activity.partitionId)) {
                MiruTenantId tenantId = activity.tenantId;
                MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, activity.partitionId, localHost);

                sticky.put(coord, true);
                ensureTopology(coord);
                ensured.add(activity.partitionId);
            }
        }
    }

    private void ensureTopology(MiruPartitionCoord coord) throws Exception {
        synchronized (topologyLock.lock(coord)) {
            if (!topology.containsKey(coord)) {
                MiruHostedPartition<BM> partition;
                if (coord.host.equals(localHost)) {
                    partition = localPartitionFactory.create(bitmaps, coord);
                } else {
                    partition = remotePartitionFactory.create(coord);
                }
                topology.put(coord, partition);
            }
        }
    }

    private void removeTopology(MiruPartitionCoord coord) throws Exception {
        synchronized (topologyLock.lock(coord)) {
            MiruHostedPartition partition = topology.remove(coord);
            if (partition != null) {
                partition.remove();
            }
        }
    }

    @Override
    public String toString() {
        return "MiruTenantTopology{" +
            "tenantId=" + tenantId +
            ", topology=" + topology.values() +
            '}';
    }

}
