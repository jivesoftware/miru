package com.jivesoftware.os.miru.service.partition.cluster;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.service.partition.MiruExpectedTenants;
import com.jivesoftware.os.miru.service.partition.MiruPartitionInfoProvider;
import com.jivesoftware.os.miru.service.partition.MiruTenantTopology;
import com.jivesoftware.os.miru.service.partition.MiruTenantTopologyFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;

/**
 *
 */
public class MiruClusterExpectedTenants implements MiruExpectedTenants {

    private final MiruPartitionInfoProvider partitionInfoProvider;
    private final MiruClusterRegistry clusterRegistry;
    private final MiruTenantTopologyFactory tenantTopologyFactory;

    private final ConcurrentMap<MiruTenantId, MiruTenantTopology<?>> hostedTenants = Maps.newConcurrentMap();
    private final Set<MiruTenantId> expectedTenants = Collections.newSetFromMap(Maps.<MiruTenantId, Boolean>newConcurrentMap());

    public MiruClusterExpectedTenants(MiruPartitionInfoProvider partitionInfoProvider,
            MiruTenantTopologyFactory tenantTopologyFactory,
            MiruClusterRegistry clusterRegistry) {

        this.partitionInfoProvider = partitionInfoProvider;
        this.tenantTopologyFactory = tenantTopologyFactory;
        this.clusterRegistry = clusterRegistry;

    }

    @Override
    public MiruTenantTopology<?> getTopology(MiruTenantId tenantId) {
        return hostedTenants.get(tenantId);
    }

    @Override
    public Collection<MiruTenantTopology<?>> topologies() {
        return hostedTenants.values();
    }

    @Override
    public boolean isExpected(MiruTenantId tenantId) {
        return expectedTenants.contains(tenantId);
    }

    @Override
    public void expect(List<MiruTenantId> expectedTenantsForHost) throws Exception {
        checkArgument(expectedTenantsForHost != null);
        for (MiruTenantId tenantId : expectedTenantsForHost) {
            MiruTenantTopology<?> tenantTopology = hostedTenants.get(tenantId);
            if (tenantTopology == null) {
                tenantTopology = tenantTopologyFactory.create(tenantId);
                hostedTenants.putIfAbsent(tenantId, tenantTopology);
            }
            Collection<MiruPartition> partitionsForTenant = clusterRegistry.getPartitionsForTenant(tenantId).values();
            for (MiruPartition partition : partitionsForTenant) {
                partitionInfoProvider.put(partition.coord, partition.info);
            }
            tenantTopology.checkForPartitionAlignment(Collections2.transform(partitionsForTenant,
                    new Function<MiruPartition, MiruPartitionCoord>() {
                        @Nullable
                        @Override
                        public MiruPartitionCoord apply(@Nullable MiruPartition input) {
                            return input != null ? input.coord : null;
                        }
                    }));

            expectedTenants.add(tenantId);
        }

        for (MiruTenantId tenantId : hostedTenants.keySet()) {
            if (!expectedTenantsForHost.contains(tenantId)) {
                expectedTenants.remove(tenantId);
                MiruTenantTopology removed = hostedTenants.remove(tenantId);
                if (removed != null) {
                    Iterator<MiruPartitionCoord> iter = partitionInfoProvider.getKeysIterator();
                    while (iter.hasNext()) {
                        MiruPartitionCoord coord = iter.next();
                        if (coord.tenantId.equals(tenantId)) {
                            iter.remove();
                        }
                    }
                    removed.remove();
                }
            }
        }
    }
}
