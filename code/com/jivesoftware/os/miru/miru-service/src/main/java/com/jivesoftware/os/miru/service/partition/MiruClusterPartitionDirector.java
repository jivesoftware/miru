package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Optional;
import com.google.common.collect.ListMultimap;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.plugin.partition.MiruHostedPartition;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionDirector;
import com.jivesoftware.os.miru.plugin.partition.OrderedPartitions;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/** @author jonathan */
public class MiruClusterPartitionDirector implements MiruPartitionDirector {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final MiruHost host;
    private final MiruClusterRegistry clusterRegistry;
    private final MiruExpectedTenants expectedTenants;

    public MiruClusterPartitionDirector(MiruHost host,
        MiruClusterRegistry clusterRegistry,
        MiruExpectedTenants expectedTenants) {
        this.host = host;
        this.clusterRegistry = clusterRegistry;
        this.expectedTenants = expectedTenants;
    }

    /** All writes enter here */
    @Override
    public void index(ListMultimap<MiruTenantId, MiruPartitionedActivity> activitiesPerTenant) throws Exception {
        for (MiruTenantId tenantId : activitiesPerTenant.keySet()) {
            if (expectedTenants.isExpected(tenantId)) {
                List<MiruPartitionedActivity> activities = activitiesPerTenant.get(tenantId);
                MiruTenantTopology tenantTopology = expectedTenants.getTopology(tenantId);
                if (tenantTopology == null) {
                    // We are not going to auto create the TenantTopology even though we know there should be one.
                } else {
                    LOG.startTimer("indexed");
                    try {
                        tenantTopology.index(activities);
                        LOG.inc("indexed", activities.size());
                        LOG.inc("indexed>" + tenantId, activities.size());
                    } finally {
                        LOG.stopTimer("indexed");
                    }
                }
            }
        }
    }

    /** All reads read from here */
    @Override
    public Iterable<? extends OrderedPartitions<?>> allQueryablePartitionsInOrder(MiruTenantId tenantId, String queryKey) throws Exception {
        MiruTenantTopology<?> topology = expectedTenants.getTopology(tenantId);
        if (topology == null) {
            return Collections.emptyList();
        }
        return topology.allPartitionsInOrder(queryKey);
    }

    @Override
    public Collection<? extends MiruHostedPartition<?>> allPartitions(MiruTenantId tenantId) throws Exception {
        MiruTenantTopology<?> topology = expectedTenants.getTopology(tenantId);
        if (topology == null) {
            return Collections.emptyList();
        }
        return topology.allPartitions();
    }

    @Override
    public Optional<MiruHostedPartition<?>> getQueryablePartition(MiruPartitionCoord miruPartitionCoord) throws Exception {
        MiruTenantTopology<?> topology = expectedTenants.getTopology(miruPartitionCoord.tenantId);
        if (topology == null) {
            return Optional.absent();
        }
        return topology.getPartition(miruPartitionCoord);
    }

    /** Updates topology timestamps */
    @Override
    public void warm(MiruTenantId tenantId) throws Exception {
        MiruTenantTopology topology = expectedTenants.getTopology(tenantId);
        if (topology != null) {
            topology.warm();
        }
    }

    /** Updates topology storage */
    @Override
    public void setStorage(MiruTenantId tenantId, MiruPartitionId partitionId, MiruBackingStorage storage) throws Exception {
        MiruTenantTopology topology = expectedTenants.getTopology(tenantId);
        if (topology != null) {
            topology.setStorageForHost(partitionId, storage, host);
        }
    }

    /** Removes host from the registry */
    @Override
    public void removeHost(MiruHost host) throws Exception {
        clusterRegistry.removeHost(host);
    }

    /** Remove topology from the registry */
    @Override
    public void removeTopology(MiruTenantId tenantId, MiruPartitionId partitionId, MiruHost host) throws Exception {
        clusterRegistry.removeTopology(tenantId, partitionId, host);
    }

    /** Check if the given tenant partition is in the desired state */
    @Override
    public boolean checkInfo(MiruTenantId tenantId, MiruPartitionId partitionId, MiruPartitionCoordInfo info) throws Exception {
        MiruTenantTopology<?> topology = expectedTenants.getTopology(tenantId);
        if (topology != null) {
            Optional<MiruHostedPartition<?>> partition = topology.getPartition(new MiruPartitionCoord(tenantId, partitionId, host));
            if (partition.isPresent()) {
                return info.state == partition.get().getState() &&
                    (info.storage == MiruBackingStorage.unknown || info.storage == partition.get().getStorage());
            }
        }
        return false;
    }

    /** MiruService calls this on a periodic interval */
    public void heartbeat() {
        try {
            long sizeInMemory = 0;
            long sizeOnDisk = 0;
            for (MiruTenantTopology<?> topology : expectedTenants.topologies()) {
                for (MiruHostedPartition<?> partition : topology.allPartitions()) {
                    try {
                        sizeInMemory += partition.sizeInMemory();
                        sizeOnDisk += partition.sizeOnDisk();
                    } catch (Exception e) {
                        LOG.warn("Failed to get size in bytes for partition " + partition, e);
                    }
                }
            }

            clusterRegistry.sendHeartbeatForHost(host, sizeInMemory, sizeOnDisk);
        } catch (Throwable t) {
            LOG.error("Heartbeat encountered a problem", t);
        }
    }

    /** MiruService calls this on a periodic interval */
    public void ensureServerPartitions() {
        try {
            List<MiruTenantId> expectedTenantsForHost = clusterRegistry.getTenantsForHost(host);
            Random random = new Random(host.hashCode());
            Collections.shuffle(expectedTenantsForHost, random);
            expectedTenants.expect(expectedTenantsForHost);
        } catch (Throwable t) {
            LOG.error("EnsureServerPartitions encountered a problem", t);
        }
    }
}
