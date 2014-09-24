package com.jivesoftware.os.miru.client.base;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruQueryServiceException;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruReadEvent;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.client.MiruActivitySenderProvider;
import com.jivesoftware.os.miru.client.MiruClient;
import com.jivesoftware.os.miru.client.MiruPartitioner;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruReplicaSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/** @author jonathan */
public class MiruBestEffortFailureTolerantClient implements MiruClient {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ExecutorService sendActivitiesToHostsThreadPool;
    private final MiruClusterRegistry clusterRegistry;
    private final MiruActivitySenderProvider activitySenderProvider;
    private final MiruPartitioner miruPartitioner;

    private final Cache<TenantAndPartitionKey, MiruReplicaSet> replicaCache;
    private final Cache<MiruTenantId, Boolean> latestAlignmentCache;

    public MiruBestEffortFailureTolerantClient(
        ExecutorService sendActivitiesToHostsThreadPool,
        MiruClusterRegistry clusterRegistry,
        MiruActivitySenderProvider activitySenderProvider,
        MiruPartitioner miruPartitioner,
        int cacheSize,
        long cacheExpiresAfterNMillis) {
        this.sendActivitiesToHostsThreadPool = sendActivitiesToHostsThreadPool;
        this.clusterRegistry = clusterRegistry;
        this.activitySenderProvider = activitySenderProvider;
        this.miruPartitioner = miruPartitioner;
        this.replicaCache = CacheBuilder.newBuilder() //TODO config
            .maximumSize(cacheSize)
            .expireAfterWrite(cacheExpiresAfterNMillis, TimeUnit.MILLISECONDS)
            .build();
        this.latestAlignmentCache = CacheBuilder.newBuilder() // TODO config
            .maximumSize(cacheSize)
            .expireAfterWrite(1, TimeUnit.MINUTES)
            .build();
    }

    @Override
    public void sendActivity(List<MiruActivity> activities, boolean recoverFromRemoval) throws Exception {
        ListMultimap<MiruTenantId, MiruActivity> activitiesPerTenant = ArrayListMultimap.create();
        for (MiruActivity activity : activities) {
            activitiesPerTenant.put(activity.tenantId, activity);
        }

        for (final MiruTenantId tenantId : activitiesPerTenant.keySet()) {
            checkForWriterAlignmentIfNecessary(tenantId);

            List<MiruActivity> tenantActivities = activitiesPerTenant.get(tenantId);
            final List<MiruPartitionedActivity> partitionTenantActivites = miruPartitioner.writeActivities(tenantId, tenantActivities, recoverFromRemoval);
            LOG.inc("sendActivity>wal", tenantActivities.size());
            LOG.inc("sendActivity>wal>tenant>" + tenantId, tenantActivities.size());

            ListMultimap<MiruPartitionId, MiruPartitionedActivity> activitiesPerPartition = ArrayListMultimap.create();
            for (MiruPartitionedActivity partitionedActivity : partitionTenantActivites) {
                activitiesPerPartition.put(partitionedActivity.partitionId, partitionedActivity);
            }

            for (final MiruPartitionId partitionId : activitiesPerPartition.keySet()) {
                TenantAndPartitionKey key = new TenantAndPartitionKey(tenantId, partitionId);
                MiruReplicaSet replicaSet = replicaCache.getIfPresent(key);

                if (replicaSet == null) {
                    try {
                        LOG.info("Refreshing replica cache for tenant:{} partition:{}", tenantId, partitionId);
                        replicaSet = clusterRegistry.getReplicaSets(tenantId, Arrays.asList(partitionId)).get(partitionId);
                        if (!replicaSet.get(MiruPartitionState.online).isEmpty()) {
                            // cache only if at least one node is online
                            replicaCache.put(key, replicaSet);
                            LOG.inc("sendActivity>cached");
                        } else {
                            LOG.warn("Failed to cache because no partitions are online for tenant:{} partition:{}", tenantId, partitionId);
                            LOG.inc("sendActivity>notCached");
                        }
                    } catch (Exception x) {
                        LOG.error("Failed to get list of hosts for tenantId:{} and partition:{}", tenantId, partitionId);
                        throw new MiruQueryServiceException("Failed to get list of hosts", x);
                    }
                } else {
                    LOG.inc("sendActivity>alreadyCached");
                }

                // This will contain all existing replicas as well as newly elected replicas
                Set<MiruHost> fullReplicaSet = electHostsForTenantPartition(tenantId, partitionId, replicaSet);
                Collection<MiruPartitionCoord> allCoords = Collections2.transform(fullReplicaSet,
                    new Function<MiruHost, MiruPartitionCoord>() {
                        @Nullable
                        @Override
                        public MiruPartitionCoord apply(@Nullable MiruHost host) {
                            return new MiruPartitionCoord(tenantId, partitionId, host);
                        }
                    });
                List<MiruPartitionedActivity> tenantPartitionedActivities = activitiesPerPartition.get(partitionId);
                sendForTenant(allCoords, tenantId, tenantPartitionedActivities);
                LOG.inc("sendActivity>sent", tenantPartitionedActivities.size());
                LOG.inc("sendActivity>sent>tenant>" + tenantId, tenantPartitionedActivities.size());
            }
        }
    }

    @Override
    public void removeActivity(List<MiruActivity> activities) throws Exception {
        ListMultimap<MiruTenantId, MiruActivity> activitiesPerTenant = ArrayListMultimap.create();
        for (MiruActivity activity : activities) {
            activitiesPerTenant.put(activity.tenantId, activity);
        }

        for (final MiruTenantId tenantId : activitiesPerTenant.keySet()) {
            checkForWriterAlignmentIfNecessary(tenantId);

            List<MiruActivity> tenantActivities = activitiesPerTenant.get(tenantId);
            miruPartitioner.removeActivities(tenantId, tenantActivities);
        }
    }

    private Set<MiruHost> electHostsForTenantPartition(MiruTenantId tenantId, MiruPartitionId partitionId, MiruReplicaSet replicaSet) throws Exception {
        if (replicaSet.getCountOfMissingReplicas() > 0) {
            return clusterRegistry.electToReplicaSetForTenantPartition(tenantId, partitionId, replicaSet);
        }
        return replicaSet.getHostsWithReplica();
    }

    @Override
    public void sendRead(MiruReadEvent readEvent) throws Exception {
        MiruTenantId tenantId = readEvent.tenantId;
        checkForWriterAlignmentIfNecessary(tenantId);
        miruPartitioner.writeReadEvent(tenantId, readEvent);
    }

    @Override
    public void sendUnread(MiruReadEvent readEvent) throws Exception {
        MiruTenantId tenantId = readEvent.tenantId;
        checkForWriterAlignmentIfNecessary(tenantId);
        miruPartitioner.writeUnreadEvent(tenantId, readEvent);
    }

    @Override
    public void sendAllRead(MiruReadEvent readEvent) throws Exception {
        MiruTenantId tenantId = readEvent.tenantId;
        checkForWriterAlignmentIfNecessary(tenantId);
        miruPartitioner.writeAllReadEvent(tenantId, readEvent);
    }

    private void sendForTenant(Collection<MiruPartitionCoord> coords, final MiruTenantId tenantId,
        final List<MiruPartitionedActivity> tenantPartitionedActivities) {

        // execute in parallel but wait for all to complete
        List<Future<?>> futures = Lists.newArrayListWithCapacity(coords.size());
        for (final MiruPartitionCoord coord : coords) {
            try {
                Future<?> future = sendActivitiesToHostsThreadPool.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            activitySenderProvider.get(coord.host).send(tenantPartitionedActivities);
                            LOG.inc("sendForTenant>sent", tenantPartitionedActivities.size());
                            LOG.inc("sendForTenant>sent>host>" + coord.host, tenantPartitionedActivities.size());
                        } catch (Exception x) {
                            LOG.warn("Failed to send {} activities for tenantId:{} to host:{}",
                                tenantPartitionedActivities.size(), tenantId, coord.host);
                            LOG.inc("sendForTenant>notSent", tenantPartitionedActivities.size());
                            LOG.inc("sendForTenant>notSent>host>" + coord.host, tenantPartitionedActivities.size());

                            // invalidate the replica cache since this host might be sick
                            //TODO also need a blacklist
                            replicaCache.invalidate(new TenantAndPartitionKey(tenantId, coord.partitionId));
                        }
                    }
                });
                futures.add(future);
                LOG.inc("sendForTenant>submitted");
            } catch (Exception x) {
                LOG.warn("Failed to submit runnable to send activities for tenantId:{} to host:{} ", tenantId, coord.host);
                LOG.inc("sendForTenant>notSubmitted");
            }
        }

        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while sending activity", e);
                Thread.interrupted();
            } catch (Exception e) {
                LOG.warn("Exception while sending activity", e);
            } finally {
                LOG.inc("sendForTenant>completed");
            }
        }

    }

    private void checkForWriterAlignmentIfNecessary(MiruTenantId tenantId) {
        // Limit how often we check for alignment per tenant
        if (latestAlignmentCache.getIfPresent(tenantId) == null) {
            try {
                latestAlignmentCache.put(tenantId, true);
                miruPartitioner.checkForAlignmentWithOtherWriters(tenantId);
                LOG.inc("alignWriters>aligned");
            } catch (Throwable t) {
                LOG.error("Unable to check for alignement with other writers", t);
                LOG.inc("alignWriters>failed");
            }
        }
    }

    private static class TenantAndPartitionKey {

        private final MiruTenantId tenantId;
        private final MiruPartitionId partitionId;

        private TenantAndPartitionKey(MiruTenantId tenantId, MiruPartitionId partitionId) {
            this.tenantId = tenantId;
            this.partitionId = partitionId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TenantAndPartitionKey that = (TenantAndPartitionKey) o;

            if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null) {
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
            return result;
        }
    }
}
