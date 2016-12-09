package com.jivesoftware.os.miru.service.partition.cluster;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruPartitionActiveUpdate;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruLocalHostedPartition;
import com.jivesoftware.os.miru.service.partition.MiruLocalPartitionFactory;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public class MiruTenantTopology<BM extends IBM, IBM> {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final long ensurePartitionsIntervalInMillis;
    private final MiruBitmaps<BM, IBM> bitmaps;
    private final MiruHost localHost;
    private final MiruTenantId tenantId;
    private final MiruLocalPartitionFactory<?, ?> localPartitionFactory;
    private final ConcurrentSkipListMap<MiruPartitionId, MiruLocalHostedPartition<BM, IBM, ?, ?>> topology;

    private final StripingLocksProvider<MiruPartitionId> topologyLock = new StripingLocksProvider<>(64);

    public MiruTenantTopology(
        long ensurePartitionsIntervalInMillis,
        MiruBitmaps<BM, IBM> bitmaps,
        MiruHost localHost,
        MiruTenantId tenantId,
        MiruLocalPartitionFactory<?, ?> localPartitionFactory) {
        this.ensurePartitionsIntervalInMillis = ensurePartitionsIntervalInMillis;
        this.bitmaps = bitmaps;
        this.localHost = localHost;
        this.tenantId = tenantId;
        this.localPartitionFactory = localPartitionFactory;
        this.topology = new ConcurrentSkipListMap<>();
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

    public void warm() throws Exception {
        Map.Entry<MiruPartitionId, MiruLocalHostedPartition<BM, IBM, ?, ?>> lastEntry = topology.lastEntry();
        if (lastEntry != null) {
            lastEntry.getValue().warm();
        }
    }

    public void warm(MiruPartitionId partitionId) throws Exception {
        MiruLocalHostedPartition<BM, IBM, ?, ?> hostedPartition = topology.get(partitionId);
        if (hostedPartition != null) {
            hostedPartition.warm();
        }
    }

    public void compact(MiruPartitionId partitionId) throws Exception {
        MiruLocalHostedPartition<BM, IBM, ?, ?> hostedPartition = topology.get(partitionId);
        if (hostedPartition != null) {
            hostedPartition.compact();
        }
    }

    public boolean rebuild(MiruPartitionId partitionId) throws Exception {
        Optional<MiruLocalHostedPartition<BM, IBM, ?, ?>> partition = getPartition(partitionId);
        return partition.isPresent() && partition.get().rebuild();
    }


    void checkForPartitionAlignment(List<MiruPartitionActiveUpdate> activeUpdates) throws Exception {
        List<MiruPartitionActiveUpdate> recentUpdatesFirst = Lists.newArrayList(activeUpdates);
        Collections.sort(recentUpdatesFirst, RECENT_UPDATES_COMPARATOR);

        for (MiruPartitionActiveUpdate activeUpdate : recentUpdatesFirst) {
            if (activeUpdate.hosted) {
                ensureTopology(MiruPartitionId.of(activeUpdate.partitionId), false);
            } else {
                removeTopology(MiruPartitionId.of(activeUpdate.partitionId));
            }
        }

        for (MiruLocalHostedPartition<BM, IBM, ?, ?> hostedPartition : topology.values()) {
            if (hostedPartition.isExpired()) {
                removeTopology(hostedPartition.getPartitionId());
            }
        }
    }

    public Collection<MiruLocalHostedPartition<BM, IBM, ?, ?>> allPartitions() {
        return Collections.unmodifiableCollection(topology.values());
    }

    public Optional<MiruLocalHostedPartition<BM, IBM, ?, ?>> getPartition(MiruPartitionId partitionId) {
        return Optional.fromNullable(topology.get(partitionId));
    }

    public void remove() {

        for (MiruHostedPartition partition : topology.values()) {
            synchronized (topologyLock.lock(partition.getPartitionId(), 0)) {
                try {
                    partition.remove();
                } catch (Exception x) {
                    LOG.error("Partition failed to remove. partition:" + partition, x);
                }
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
                ensureTopology(activity.partitionId, true);
                ensured.add(activity.partitionId);
            }
        }
    }

    private void ensureTopology(MiruPartitionId partitionId, boolean expires) throws Exception {
        synchronized (topologyLock.lock(partitionId, 0)) {
            MiruLocalHostedPartition<BM, IBM, ?, ?> hostedPartition = topology.get(partitionId);
            if (hostedPartition != null) {
                if (!expires) {
                    hostedPartition.cancelExpiration();
                }
            } else {
                MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, localHost);
                long expireAfterMillis = expires ? ensurePartitionsIntervalInMillis * 2 : -1;
                //TODO unnecessary cast, but the wildcards cause some IDE confusion
                MiruLocalHostedPartition<BM, IBM, ?, ?> partition = localPartitionFactory.create((MiruBitmaps) bitmaps, coord, expireAfterMillis);
                topology.put(partitionId, partition);
            }
        }
    }

    private void removeTopology(MiruPartitionId partitionId) throws Exception {
        synchronized (topologyLock.lock(partitionId, 0)) {
            MiruHostedPartition partition = topology.remove(partitionId);
            if (partition != null) {
                partition.remove();
            }
        }
    }

    private static final Comparator<MiruPartitionActiveUpdate> RECENT_UPDATES_COMPARATOR = (u1, u2) -> {
        return Integer.compare(u2.partitionId, u1.partitionId); // reversed for descending order
    };

    @Override
    public String toString() {
        return "MiruTenantTopology{"
            + "tenantId=" + tenantId
            + ", topology=" + topology.values()
            + '}';
    }

}
