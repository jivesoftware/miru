/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.miru.cluster;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 *
 * @author jonathan.colt
 */
public class MiruReplicaSetDirector {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider orderIdProvider;
    private final MiruClusterRegistry clusterRegistry;

    public MiruReplicaSetDirector(OrderIdProvider orderIdProvider,
        MiruClusterRegistry clusterRegistry) {
        this.orderIdProvider = orderIdProvider;
        this.clusterRegistry = clusterRegistry;
    }

    public Set<MiruHost> electToReplicaSetForTenantPartition(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        MiruReplicaSet replicaSet,
        final long eligibleAfter) throws Exception {

        LinkedHashSet<MiruClusterRegistry.HostHeartbeat> hostHeartbeats = clusterRegistry.getAllHosts();

        Set<MiruHost> hostsWithReplica = Sets.newHashSet(replicaSet.getHostsWithReplica());
        int countOfMissingReplicas = replicaSet.getCountOfMissingReplicas();

        MiruHost[] hostsArray = FluentIterable.from(hostHeartbeats)
            .filter(new Predicate<MiruClusterRegistry.HostHeartbeat>() {
                @Override
                public boolean apply(@Nullable MiruClusterRegistry.HostHeartbeat input) {
                    return input != null && input.heartbeat > eligibleAfter;
                }
            })
            .transform(new Function<MiruClusterRegistry.HostHeartbeat, MiruHost>() {
                @Nullable
                @Override
                public MiruHost apply(@Nullable MiruClusterRegistry.HostHeartbeat input) {
                    return input.host;
                }
            })
            .toArray(MiruHost.class);

        // We use a hash code derived from the tenant and partition to choose a starting index,
        // and start electing eligible hosts from there.
        // TODO since we only consider elegible host we should add a cleanup task that makes replica set contiguios with respect to the ring.
        int hashCode = Objects.hash(tenantId, partitionId);
        int electedCount = 0;
        int index = Math.abs(hashCode);
        for (int i = 0; i < hostsArray.length && electedCount < countOfMissingReplicas; i++, index++) {
            MiruHost hostToElect = hostsArray[index % hostsArray.length];
            if (!hostsWithReplica.contains(hostToElect)) {
                // this host is eligible
                clusterRegistry.ensurePartitionCoord(new MiruPartitionCoord(tenantId, partitionId, hostToElect));
                clusterRegistry.addToReplicaRegistry(tenantId, partitionId, Long.MAX_VALUE - orderIdProvider.nextId(), hostToElect);
                hostsWithReplica.add(hostToElect);
                electedCount++;

                LOG.debug("Elected {} to {} on {}", new Object[]{ hostToElect, tenantId, partitionId });
            }
        }
        return hostsWithReplica;
    }

    public void removeReplicas(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        clusterRegistry.removeTenantPartionReplicaSet(tenantId, partitionId);
    }

    public void moveReplica(MiruTenantId tenantId, MiruPartitionId partitionId, Optional<MiruHost> fromHost, MiruHost toHost) throws Exception {
        Map<MiruPartitionId, MiruReplicaSet> replicaSets = clusterRegistry.getReplicaSets(tenantId, Arrays.asList(partitionId));
        MiruReplicaSet replicaSet = replicaSets.get(partitionId);

        List<MiruHost> hosts = Lists.newArrayList(replicaSet.getHostsWithReplica());
        if (fromHost.isPresent() && !hosts.isEmpty()) {
            // list is small enough that direct removal is better than copying from intermediate set
            hosts.remove(fromHost.or(hosts.get(0)));
        }
        hosts.add(toHost);

        for (MiruHost host : hosts) {
            clusterRegistry.ensurePartitionCoord(new MiruPartitionCoord(tenantId, partitionId, host));
            clusterRegistry.addToReplicaRegistry(tenantId, partitionId, Long.MAX_VALUE - orderIdProvider.nextId(), host);
        }
    }

}
