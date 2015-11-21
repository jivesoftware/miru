package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.activity.CoordinateStream;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionDirector;
import com.jivesoftware.os.miru.plugin.partition.MiruQueryablePartition;
import com.jivesoftware.os.miru.plugin.partition.OrderedPartitions;
import com.jivesoftware.os.miru.service.partition.cluster.MiruTenantTopology;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.List;

/** @author jonathan */
public class MiruClusterPartitionDirector implements MiruPartitionDirector {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruHost host;
    private final MiruClusterClient clusterClient;
    private final MiruExpectedTenants expectedTenants;

    public MiruClusterPartitionDirector(MiruHost host,
        MiruClusterClient clusterClient,
        MiruExpectedTenants expectedTenants) {
        this.host = host;
        this.clusterClient = clusterClient;
        this.expectedTenants = expectedTenants;
    }

    /** All writes enter here */
    @Override
    public void index(ListMultimap<MiruTenantId, MiruPartitionedActivity> activitiesPerTenant) throws Exception {
        for (MiruTenantId tenantId : activitiesPerTenant.keySet()) {
            MiruTenantTopology tenantTopology = expectedTenants.getLocalTopology(tenantId);
            if (tenantTopology != null) {
                List<MiruPartitionedActivity> activities = activitiesPerTenant.get(tenantId);
                LOG.startTimer("indexed");
                try {
                    tenantTopology.index(activities);
                    LOG.inc("indexed", activities.size());
                    LOG.inc("indexed", activities.size(), tenantId.toString());
                } finally {
                    LOG.stopTimer("indexed");
                }
            }
        }
    }

    /** All reads read from here */
    @Override
    public Iterable<? extends OrderedPartitions<?, ?>> allQueryablePartitionsInOrder(MiruTenantId tenantId, String queryKey) throws Exception {
        return expectedTenants.allQueryablePartitionsInOrder(tenantId, queryKey);
    }

    @Override
    public Optional<? extends MiruQueryablePartition<?, ?>> getQueryablePartition(MiruPartitionCoord miruPartitionCoord) throws Exception {
        MiruTenantTopology<?, ?> topology = expectedTenants.getLocalTopology(miruPartitionCoord.tenantId);
        if (topology == null) {
            return Optional.absent();
        }
        return topology.getPartition(miruPartitionCoord.partitionId);
    }

    /** Updates topology timestamps */
    @Override
    public void warm(MiruTenantId tenantId) throws Exception {
        MiruTenantTopology topology = expectedTenants.getLocalTopology(tenantId);
        if (topology != null) {
            topology.warm();
        }
    }

    /** Updates topology storage */
    @Override
    public void setStorage(MiruTenantId tenantId, MiruPartitionId partitionId, MiruBackingStorage storage) throws Exception {
        MiruTenantTopology topology = expectedTenants.getLocalTopology(tenantId);
        if (topology != null) {
            byte[] primitiveBuffer = new byte[8];
            topology.setStorage(partitionId, storage, primitiveBuffer);
        }
    }

    /** Removes host from the registry */
    @Override
    public void removeHost(MiruHost host) throws Exception {
        clusterClient.removeHost(host);
    }

    /** Remove topology from the registry */
    @Override
    public void removeTopology(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) throws Exception {
        clusterClient.removeTopology(host, tenantId, partitionId);
    }

    /** Check if the given tenant partition is in the desired state */
    @Override
    public boolean checkInfo(MiruTenantId tenantId, MiruPartitionId partitionId, MiruPartitionCoordInfo info) throws Exception {
        MiruTenantTopology<?, ?> topology = expectedTenants.getLocalTopology(tenantId);
        if (topology != null) {
            Optional<? extends MiruLocalHostedPartition<?, ?, ?, ?>> partition = topology.getPartition(partitionId);
            if (partition.isPresent()) {
                return info.state == partition.get().getState()
                    && (info.storage == MiruBackingStorage.unknown || info.storage == partition.get().getStorage());
            }
        }
        return false;
    }

    /** If the given coordinate is expected on this host, prioritizes the partition's rebuild. */
    @Override
    public boolean prioritizeRebuild(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        return expectedTenants.prioritizeRebuild(new MiruPartitionCoord(tenantId, partitionId, host));
    }

    @Override
    public boolean expectedTopologies(Optional<MiruTenantId> tenantId, CoordinateStream stream) throws Exception {
        return expectedTenants.expectedTopologies(tenantId, stream);
    }

    /** MiruService calls this on a periodic interval */
    public void heartbeat() {
        try {
            expectedTenants.thumpthump();
        } catch (Throwable t) {
            LOG.error("Heartbeat encountered a problem", t);
        }
    }
}
