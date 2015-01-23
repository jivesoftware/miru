package com.jivesoftware.os.miru.cluster.rcvs;

import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruTopologyStatus;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.MiruReplicaSet;
import com.jivesoftware.os.miru.cluster.MiruReplicaSetDirector;
import com.jivesoftware.os.miru.cluster.MiruTenantConfigFields;
import com.jivesoftware.os.rcvs.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.rcvs.api.timestamper.Timestamper;
import com.jivesoftware.os.rcvs.inmemory.InMemoryRowColumnValueStore;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class MiruRCVSClusterRegistryTest {

    private final int numReplicas = 3;
    private final MiruTenantId tenantId = new MiruTenantId(new byte[]{ 1, 2, 3, 4 });
    private final MiruPartitionId partitionId = MiruPartitionId.of(0);

    private Timestamper timestamper = new CurrentTimestamper();
    private MiruReplicaSetDirector replicaSetDirector;
    private MiruRCVSClusterRegistry registry;

    @BeforeMethod
    public void setUp() throws Exception {
        registry = new MiruRCVSClusterRegistry(
            timestamper,
            new InMemoryRowColumnValueStore<MiruVoidByte, MiruHost, MiruHostsColumnKey, MiruHostsColumnValue>(),
            new InMemoryRowColumnValueStore<MiruVoidByte, MiruHost, MiruTenantId, MiruVoidByte>(),
            new InMemoryRowColumnValueStore<MiruTenantId, MiruHost, MiruPartitionId, MiruVoidByte>(),
            new InMemoryRowColumnValueStore<MiruTenantId, MiruPartitionId, Long, MiruHost>(),
            new InMemoryRowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTopologyColumnKey, MiruTopologyColumnValue>(),
            new InMemoryRowColumnValueStore<MiruVoidByte, MiruTenantId, MiruTenantConfigFields, Long>(),
            new InMemoryRowColumnValueStore<MiruVoidByte, MiruTenantId, Integer, MiruPartitionId>(),
            numReplicas,
            TimeUnit.HOURS.toMillis(1));
        replicaSetDirector = new MiruReplicaSetDirector(new OrderIdProviderImpl(new ConstantWriterIdProvider(1)), registry);
    }

    @Test
    public void testUpdateAndGetTopology() throws Exception {
        MiruHost[] hosts = addHosts(1);

        Set<MiruHost> electedHosts = replicaSetDirector.electToReplicaSetForTenantPartition(tenantId,
            partitionId,
            new MiruReplicaSet(ArrayListMultimap.<MiruPartitionState, MiruPartition>create(), Sets.<MiruHost>newHashSet(), numReplicas),
            timestamper.get() - TimeUnit.HOURS.toMillis(1));
        assertEquals(electedHosts.size(), 1);

        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, hosts[0]);
        registry.updateTopology(coord, Optional.of(new MiruPartitionCoordInfo(MiruPartitionState.online, MiruBackingStorage.disk)),
            Optional.of(timestamper.get()));

        List<MiruTopologyStatus> topologyStatusForTenantHost = registry.getTopologyStatusForTenantHost(tenantId, hosts[0]);
        List<MiruTopologyStatus> onlineStatus = Lists.newArrayList();
        for (MiruTopologyStatus status : topologyStatusForTenantHost) {
            if (status.partition.info.state == MiruPartitionState.online) {
                onlineStatus.add(status);
            }
        }
        assertEquals(onlineStatus.size(), 1);

        MiruTopologyStatus status = onlineStatus.get(0);
        assertEquals(status.partition.coord, coord);
        assertEquals(status.partition.info.storage, MiruBackingStorage.disk);
    }

    @Test
    public void testRefreshAndGetTopology() throws Exception {
        MiruHost[] hosts = addHosts(1);

        Set<MiruHost> electedHosts = replicaSetDirector.electToReplicaSetForTenantPartition(tenantId,
            partitionId,
            new MiruReplicaSet(ArrayListMultimap.<MiruPartitionState, MiruPartition>create(), Sets.<MiruHost>newHashSet(), numReplicas),
            timestamper.get() - TimeUnit.HOURS.toMillis(1));
        assertEquals(electedHosts.size(), 1);

        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, hosts[0]);
        registry.updateTopology(coord, Optional.<MiruPartitionCoordInfo>absent(), Optional.of(timestamper.get()));

        List<MiruTopologyStatus> topologyStatusForTenantHost = registry.getTopologyStatusForTenantHost(tenantId, hosts[0]);
        List<MiruTopologyStatus> offlineStatus = Lists.newArrayList();
        for (MiruTopologyStatus status : topologyStatusForTenantHost) {
            if (status.partition.info.state == MiruPartitionState.offline) {
                offlineStatus.add(status);
            }
        }

        assertEquals(offlineStatus.size(), 1);

        MiruTopologyStatus status = offlineStatus.get(0);
        assertEquals(status.partition.coord, coord);
        assertEquals(status.partition.info.storage, MiruBackingStorage.memory);
    }

    @Test
    public void testElectAndMoveReplica() throws Exception {
        MiruHost[] hosts = addHosts(4);

        Set<MiruHost> electedHosts = replicaSetDirector.electToReplicaSetForTenantPartition(tenantId,
            partitionId,
            new MiruReplicaSet(ArrayListMultimap.<MiruPartitionState, MiruPartition>create(), Sets.<MiruHost>newHashSet(), numReplicas),
            timestamper.get() - TimeUnit.HOURS.toMillis(1));

        MiruReplicaSet replicaSet = registry.getReplicaSets(tenantId, Arrays.asList(partitionId)).get(partitionId);

        assertEquals(replicaSet.getHostsWithReplica(), electedHosts);

        Set<MiruHost> unelectedHosts = Sets.newHashSet(hosts);
        unelectedHosts.removeAll(electedHosts);

        assertEquals(unelectedHosts.size(), 1);

        MiruHost fromHost = electedHosts.iterator().next();
        MiruHost toHost = unelectedHosts.iterator().next();

        replicaSetDirector.moveReplica(tenantId, partitionId, Optional.of(fromHost), toHost);

        replicaSet = registry.getReplicaSets(tenantId, Arrays.asList(partitionId)).get(partitionId);

        assertEquals(replicaSet.getHostsWithReplica().size(), numReplicas);
        assertFalse(replicaSet.getHostsWithReplica().contains(fromHost));
        assertTrue(replicaSet.getHostsWithReplica().contains(toHost));
    }

    private MiruHost[] addHosts(int numHosts) throws Exception {
        MiruHost[] hosts = new MiruHost[numHosts];
        for (int i = 0; i < numHosts; i++) {
            hosts[i] = new MiruHost("localhost", 49_600 + i);
            registry.sendHeartbeatForHost(hosts[i]);
        }
        return hosts;
    }
}
