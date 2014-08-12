package com.jivesoftware.os.miru.service.partition.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.partition.MiruExpectedTenants;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartitionComparison;
import com.jivesoftware.os.miru.service.partition.MiruLocalPartitionFactory;
import com.jivesoftware.os.miru.service.partition.MiruPartitionEventHandler;
import com.jivesoftware.os.miru.service.partition.MiruPartitionInfoProvider;
import com.jivesoftware.os.miru.service.partition.MiruRemotePartitionFactory;
import com.jivesoftware.os.miru.service.partition.MiruTenantTopology;
import com.jivesoftware.os.miru.service.partition.MiruTenantTopologyFactory;
import com.jivesoftware.os.miru.service.stream.MiruStreamFactory;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import static com.google.common.base.Preconditions.checkArgument;

/**
 *
 */
@Singleton
public class MiruClusterExpectedTenants implements MiruExpectedTenants {

    private final MiruClusterRegistry clusterRegistry;
    private final MiruHostedPartitionComparison partitionComparison;
    private final MiruTenantTopologyFactory tenantTopologyFactory;

    private final ConcurrentMap<MiruTenantId, MiruTenantTopology> hostedTenants = Maps.newConcurrentMap();
    private final Set<MiruTenantId> expectedTenants = Collections.newSetFromMap(Maps.<MiruTenantId, Boolean>newConcurrentMap());
    private final ConcurrentMap<MiruPartitionCoord, MiruPartitionCoordInfo> coordToInfo = Maps.newConcurrentMap();

    @Inject
    public MiruClusterExpectedTenants(
        MiruServiceConfig config,
        @Named("miruServiceHost") MiruHost host,
        @Named("miruScheduledExecutor") ScheduledExecutorService scheduledExecutorService,
        MiruClusterRegistry clusterRegistry,
        MiruHostedPartitionComparison partitionComparison,
        MiruStreamFactory streamFactory,
        MiruActivityWALReader activityWALReader,
        MiruPartitionEventHandler partitionEventHandler,
        HttpClientFactory httpClientFactory,
        ObjectMapper objectMapper) {

        MiruLocalPartitionFactory localPartitionFactory = new MiruLocalPartitionFactory(config, streamFactory, activityWALReader, partitionEventHandler,
            scheduledExecutorService);
        MiruRemotePartitionFactory remotePartitionFactory = new MiruRemotePartitionFactory(new CachedClusterPartitionInfoProvider(), httpClientFactory,
            objectMapper);

        this.clusterRegistry = clusterRegistry;
        this.partitionComparison = partitionComparison;

        this.tenantTopologyFactory = new MiruTenantTopologyFactory(config, host, localPartitionFactory, remotePartitionFactory, this.partitionComparison);
    }

    private class CachedClusterPartitionInfoProvider implements MiruPartitionInfoProvider {

        @Override
        public Optional<MiruPartitionCoordInfo> get(MiruPartitionCoord coord) {
            return Optional.fromNullable(coordToInfo.get(coord));
        }
    }

    @Override
    public MiruTenantTopology getTopology(MiruTenantId tenantId) {
        return hostedTenants.get(tenantId);
    }

    @Override
    public Collection<MiruTenantTopology> topologies() {
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
            MiruTenantTopology tenantTopology = hostedTenants.get(tenantId);
            if (tenantTopology == null) {
                tenantTopology = tenantTopologyFactory.create(tenantId);
                hostedTenants.putIfAbsent(tenantId, tenantTopology);
            }
            Collection<MiruPartition> partitionsForTenant = clusterRegistry.getPartitionsForTenant(tenantId).values();
            for (MiruPartition partition : partitionsForTenant) {
                coordToInfo.put(partition.coord, partition.info);
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
                    Iterator<MiruPartitionCoord> iter = coordToInfo.keySet().iterator();
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
