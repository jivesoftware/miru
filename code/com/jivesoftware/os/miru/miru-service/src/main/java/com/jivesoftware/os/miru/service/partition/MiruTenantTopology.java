package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Collections2;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

public class MiruTenantTopology<BM> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruBitmaps<BM> bitmaps;
    private final MiruHost localHost;
    private final MiruTenantId tenantId;
    private final MiruLocalPartitionFactory localPartitionFactory;
    private final MiruRemotePartitionFactory remotePartitionFactory;
    private final ConcurrentSkipListMap<PartitionAndHost, MiruHostedPartition<BM>> topology;
    private final Cache<PartitionAndHost, Boolean> sticky;
    private final MiruHostedPartitionComparison partitionComparison;

    private final StripingLocksProvider<PartitionAndHost> topologyLock = new StripingLocksProvider<>(64);

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
        this.topology = new ConcurrentSkipListMap<>();
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
        Set<PartitionAndHost> expected = Sets.newHashSet(Collections2.transform(coordsForTenantHost, new Function<MiruPartitionCoord, PartitionAndHost>() {
            @Override
            public PartitionAndHost apply(MiruPartitionCoord input) {
                return new PartitionAndHost(input.partitionId, input.host);
            }
        }));
        Set<PartitionAndHost> adding;
        Set<PartitionAndHost> removing;
        synchronized (topology) {
            Set<PartitionAndHost> knownCoords = topology.keySet();
            adding = Sets.newHashSet(expected);
            adding.removeAll(knownCoords);

            removing = Sets.newHashSet(knownCoords);
            removing.removeAll(expected);
            removing.removeAll(sticky.asMap().keySet());
        }

        for (PartitionAndHost add : adding) {
            ensureTopology(add);
        }

        for (PartitionAndHost remove : removing) {
            removeTopology(remove);
        }
    }

    public Collection<? extends MiruHostedPartition<?>> allPartitions() {
        return Collections.unmodifiableCollection(topology.values());
    }

    public Iterable<OrderedPartitions<BM>> allPartitionsInOrder(String queryKey) {
        List<MiruHostedPartition<BM>> allPartitions = Lists.newArrayList(topology.values());

        ListMultimap<MiruPartitionId, MiruHostedPartition<BM>> partitionsPerId = Multimaps.index(allPartitions,
            new Function<MiruHostedPartition<BM>, MiruPartitionId>() {
                @Override
                public MiruPartitionId apply(MiruHostedPartition<BM> input) {
                    return input.getPartitionId();
                }
            });

        List<OrderedPartitions<BM>> allOrderedPartitions = Lists.newArrayList();
        List<MiruPartitionId> partitionIds = Lists.newArrayList(partitionsPerId.keySet());
        Collections.sort(partitionIds);
        Collections.reverse(partitionIds);
        for (MiruPartitionId partitionId : partitionIds) {
            List<MiruHostedPartition<BM>> partitions = partitionsPerId.get(partitionId);
            List<MiruHostedPartition<BM>> orderedPartitions = partitionComparison.orderPartitions(tenantId, partitionId, queryKey, partitions);
            allOrderedPartitions.add(new OrderedPartitions<>(tenantId, partitionId, orderedPartitions));
        }

        return allOrderedPartitions;
    }

    public Optional<MiruHostedPartition<?>> getPartition(MiruPartitionCoord miruPartitionCoord) {
        MiruHostedPartition<?> partition = topology.get(new PartitionAndHost(miruPartitionCoord.partitionId, miruPartitionCoord.host));
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
                PartitionAndHost partitionAndHost = new PartitionAndHost(activity.partitionId, localHost);

                sticky.put(partitionAndHost, true);
                ensureTopology(partitionAndHost);
                ensured.add(activity.partitionId);
            }
        }
    }

    private void ensureTopology(PartitionAndHost partitionAndHost) throws Exception {
        synchronized (topologyLock.lock(partitionAndHost)) {
            if (!topology.containsKey(partitionAndHost)) {
                MiruHostedPartition<BM> partition;
                MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionAndHost.partitionId, partitionAndHost.host);
                if (partitionAndHost.host.equals(localHost)) {
                    partition = localPartitionFactory.create(bitmaps, coord);
                } else {
                    partition = remotePartitionFactory.create(coord);
                }
                topology.put(partitionAndHost, partition);
            }
        }
    }

    private void removeTopology(PartitionAndHost partitionAndHost) throws Exception {
        synchronized (topologyLock.lock(partitionAndHost)) {
            MiruHostedPartition partition = topology.remove(partitionAndHost);
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
