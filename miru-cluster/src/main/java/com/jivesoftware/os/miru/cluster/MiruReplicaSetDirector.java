/*
 * $Revision$
 * $Date$
 *
 * Copyright (C) 1999-$year$ Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package com.jivesoftware.os.miru.cluster;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.HostHeartbeat;
import com.jivesoftware.os.miru.api.topology.MiruReplicaHosts;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
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

    public void elect(MiruHost host, MiruTenantId tenantId, MiruPartitionId partitionId, long electionId) throws Exception {
        ListMultimap<MiruHost, TenantAndPartition> coords = ArrayListMultimap.create();
        coords.put(host, new TenantAndPartition(tenantId, partitionId));
        clusterRegistry.ensurePartitionCoords(coords);
        clusterRegistry.addToReplicaRegistry(coords, electionId);
    }

    public Set<MiruHost> electHostsForTenantPartition(MiruTenantId tenantId, MiruPartitionId partitionId, MiruReplicaSet replicaSet) throws Exception {
        if (replicaSet.getCountOfMissingReplicas() > 0) {
            return electToReplicaSetForTenantPartition(tenantId,
                partitionId,
                replicaSet,
                System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)); // TODO expose to config!
        }
        return replicaSet.getHostsWithReplica();
    }

    private Set<MiruHost> electToReplicaSetForTenantPartition(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        MiruReplicaSet replicaSet,
        final long eligibleAfter) throws Exception {

        LinkedHashSet<HostHeartbeat> hostHeartbeats = clusterRegistry.getAllHosts();

        Set<MiruHost> hostsWithReplica = Sets.newHashSet(replicaSet.getHostsWithReplica());
        int countOfMissingReplicas = replicaSet.getCountOfMissingReplicas();

        MiruHost[] hostsArray = hostHeartbeats.stream()
            .filter(input -> input != null && input.heartbeat > eligibleAfter)
            .map(input -> input.host)
            .toArray(MiruHost[]::new);

        // We use a hash code derived from the tenant and partition to choose a starting index,
        // and start electing eligible hosts from there.
        // TODO since we only consider eligible hosts we should add a cleanup task that makes replica sets contiguous with respect to the ring.
        int hashCode = Objects.hash(tenantId, partitionId);
        int electedCount = 0;
        int index = hashCode == Integer.MIN_VALUE ? Integer.MAX_VALUE : Math.abs(hashCode);
        for (int i = 0; i < hostsArray.length && electedCount < countOfMissingReplicas; i++, index++) {
            MiruHost hostToElect = hostsArray[index % hostsArray.length];
            if (!hostsWithReplica.contains(hostToElect)) {
                // this host is eligible
                elect(hostToElect, tenantId, partitionId, Long.MAX_VALUE - orderIdProvider.nextId());
                hostsWithReplica.add(hostToElect);
                electedCount++;

                LOG.debug("Elected {} to {} on {}", new Object[] { hostToElect, tenantId, partitionId });
            }
        }
        return hostsWithReplica;
    }

    public void removeReplicas(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        clusterRegistry.removeTenantPartionReplicaSet(tenantId, partitionId);
    }

    public void moveReplica(MiruTenantId tenantId, MiruPartitionId partitionId, Optional<MiruHost> fromHost, MiruHost toHost) throws Exception {
        MiruReplicaHosts replicaHosts = replicas(tenantId, partitionId);

        List<MiruHost> hosts = new ArrayList<>(replicaHosts.replicaHosts);
        if (fromHost.isPresent() && !hosts.isEmpty()) {
            // list is small enough that direct removal is better than copying from intermediate set
            hosts.remove(fromHost.or(hosts.get(0)));
        }
        hosts.add(toHost);

        for (MiruHost host : hosts) {
            elect(host, tenantId, partitionId, Long.MAX_VALUE - orderIdProvider.nextId());
        }
    }

    private MiruReplicaHosts replicas(MiruTenantId tenantId, MiruPartitionId partitionId) throws Exception {
        Map<MiruPartitionId, MiruReplicaSet> replicaSets = clusterRegistry.getReplicaSets(tenantId, Collections.singletonList(partitionId));
        MiruReplicaSet replicaSet = replicaSets.get(partitionId);

        return new MiruReplicaHosts(!replicaSet.get(MiruPartitionState.online).isEmpty(),
            replicaSet.getHostsWithReplica(),
            replicaSet.getCountOfMissingReplicas());
    }

}
