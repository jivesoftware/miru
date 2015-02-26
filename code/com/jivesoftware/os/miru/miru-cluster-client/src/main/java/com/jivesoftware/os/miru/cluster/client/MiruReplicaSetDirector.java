/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.miru.cluster.client;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.HostHeartbeat;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruReplicaHosts;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 *
 * @author jonathan.colt
 */
public class MiruReplicaSetDirector {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider orderIdProvider;
    private final MiruClusterClient clusterClient;

    public MiruReplicaSetDirector(OrderIdProvider orderIdProvider,
        MiruClusterClient clusterClient) {
        this.orderIdProvider = orderIdProvider;
        this.clusterClient = clusterClient;
    }

    public Set<MiruHost> electToReplicaSetForTenantPartition(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        MiruReplicaHosts replicaHosts,
        final long eligibleAfter) throws Exception {

        List<HostHeartbeat> hostHeartbeats = clusterClient.allhosts();

        Set<MiruHost> hostsWithReplica = Sets.newHashSet(replicaHosts.replicaHosts);
        int countOfMissingReplicas = replicaHosts.countOfMissingReplicas;

        MiruHost[] hostsArray = FluentIterable.from(hostHeartbeats)
            .filter(new Predicate<HostHeartbeat>() {
                @Override
                public boolean apply(HostHeartbeat input) {
                    return input != null && input.heartbeat > eligibleAfter;
                }
            })
            .transform(new Function<HostHeartbeat, MiruHost>() {
                @Override
                public MiruHost apply(HostHeartbeat input) {
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
                clusterClient.elect(hostToElect, tenantId, partitionId, Long.MAX_VALUE - orderIdProvider.nextId());
                hostsWithReplica.add(hostToElect);
                electedCount++;

                LOG.debug("Elected {} to {} on {}", new Object[]{hostToElect, tenantId, partitionId});
            }
        }
        return hostsWithReplica;
    }

    public void removeReplicas(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        clusterClient.removeReplica(tenantId, partitionId);
    }

    public void moveReplica(MiruTenantId tenantId, MiruPartitionId partitionId, Optional<MiruHost> fromHost, MiruHost toHost) throws Exception {
        MiruReplicaHosts replicaHosts = clusterClient.replicas(tenantId, partitionId);

        List<MiruHost> hosts = new ArrayList<>(replicaHosts.replicaHosts);
        if (fromHost.isPresent() && !hosts.isEmpty()) {
            // list is small enough that direct removal is better than copying from intermediate set
            hosts.remove(fromHost.or(hosts.get(0)));
        }
        hosts.add(toHost);

        for (MiruHost host : hosts) {
            clusterClient.elect(host, tenantId, partitionId, Long.MAX_VALUE - orderIdProvider.nextId());
        }
    }

}
