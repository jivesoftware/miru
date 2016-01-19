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
import com.google.common.collect.Maps;
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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author jonathan.colt
 */
public class MiruReplicaSetDirector {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final OrderIdProvider orderIdProvider;
    private final MiruClusterRegistry clusterRegistry;
    private final HostDescriptorProvider hostDescriptorProvider;

    public MiruReplicaSetDirector(OrderIdProvider orderIdProvider,
        MiruClusterRegistry clusterRegistry,
        HostDescriptorProvider hostDescriptorProvider) {
        this.orderIdProvider = orderIdProvider;
        this.clusterRegistry = clusterRegistry;
        this.hostDescriptorProvider = hostDescriptorProvider;
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
        long eligibleAfter) throws Exception {

        LinkedHashSet<HostHeartbeat> hostHeartbeats = clusterRegistry.getAllHosts();

        Set<MiruHost> hostsWithReplica = Sets.newHashSet(replicaSet.getHostsWithReplica());
        int hashCode = Objects.hash(tenantId, partitionId);

        Map<MiruHost, String> hostToRack = Maps.newHashMap();
        hostDescriptorProvider.stream((datacenter, rack, host) -> {
            hostToRack.put(host, rack);
            return true;
        });

        ListMultimap<String, MiruHost> perRackCurrent = ArrayListMultimap.create();
        for (MiruHost host : hostsWithReplica) {
            String rack = hostToRack.getOrDefault(host, "");
            perRackCurrent.put(rack, host);
        }

        Map<String, List<MiruHost>> perRackHosts = new HashMap<>();
        for (HostHeartbeat hostHeartbeat : hostHeartbeats) {
            if (hostHeartbeat.heartbeat > eligibleAfter) {
                String rack = hostToRack.get(hostHeartbeat.host);
                if (rack != null) {
                    perRackHosts.computeIfAbsent(rack, (key) -> new ArrayList<>()).add(hostHeartbeat.host);
                } else {
                    LOG.warn("Ignored host with missing rack {}", hostHeartbeat.host);
                }
            }
        }

        Random random = new Random(new Random(hashCode).nextLong());
        for (List<MiruHost> rackMembers : perRackHosts.values()) {
            Collections.shuffle(rackMembers, random);
        }

        List<String> racks = new ArrayList<>(perRackHosts.keySet());
        int desiredNumberOfReplicas = replicaSet.getDesiredNumberOfReplicas();

        while (perRackCurrent.size() < desiredNumberOfReplicas) {
            Collections.sort(racks, (o1, o2) -> Integer.compare(perRackCurrent.get(o1).size(), perRackCurrent.get(o2).size()));
            boolean advanced = false;
            for (String cycleRack : racks) {
                List<MiruHost> rackHosts = perRackHosts.get(cycleRack);
                if (!rackHosts.isEmpty()) {
                    perRackCurrent.put(cycleRack, rackHosts.remove(rackHosts.size() - 1));
                    advanced = true;
                    break;
                }
            }
            if (!advanced) {
                break;
            }
        }

        if (perRackCurrent.size() < desiredNumberOfReplicas) {
            LOG.error("Tenant {} partition {} required {} replicas but only {} hosts are available",
                tenantId, partitionId, desiredNumberOfReplicas, perRackCurrent.size());
        }

        for (MiruHost hostToElect : perRackCurrent.values()) {
            if (hostsWithReplica.add(hostToElect)) {
                elect(hostToElect, tenantId, partitionId, Long.MAX_VALUE - orderIdProvider.nextId());
            }
            LOG.debug("Elected {} to {} on {}", hostToElect, tenantId, partitionId);
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

        return new MiruReplicaHosts(!replicaSet.hasPartitionInStates(MiruPartitionState.onlineStates()),
            replicaSet.getHostsWithReplica(),
            replicaSet.getCountOfMissingReplicas());
    }

}
