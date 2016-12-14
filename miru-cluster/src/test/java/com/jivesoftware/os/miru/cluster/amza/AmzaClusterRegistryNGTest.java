package com.jivesoftware.os.miru.cluster.amza;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceConfig;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceInitializer;
import com.jivesoftware.os.miru.amza.NoOpClientHealth;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruHostProvider;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.api.marshall.MiruVoidByte;
import com.jivesoftware.os.miru.api.topology.MiruIngressUpdate;
import com.jivesoftware.os.miru.api.topology.MiruTopologyStatus;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruReplicaSet;
import com.jivesoftware.os.miru.cluster.MiruReplicaSetDirector;
import com.jivesoftware.os.miru.cluster.rcvs.MiruSchemaColumnKey;
import com.jivesoftware.os.rcvs.inmemory.InMemoryRowColumnValueStore;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckRegistry;
import com.jivesoftware.os.routing.bird.health.api.HealthChecker;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.merlin.config.BindInterfaceToConfiguration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author jonathan.colt
 */
public class AmzaClusterRegistryNGTest {

    private final int numReplicas = 3;
    private final MiruTenantId tenantId = new MiruTenantId(new byte[] { 1, 2, 3, 4 });
    private final MiruPartitionId partitionId = MiruPartitionId.of(0);

    private MiruReplicaSetDirector replicaSetDirector;
    private MiruClusterRegistry registry;
    private List<MiruHost> hosts;

    @BeforeMethod
    public void setUp() throws Exception {
        HealthFactory.initialize(BindInterfaceToConfiguration::bindDefault,
            new HealthCheckRegistry() {

                @Override
                public void register(HealthChecker healthChecker) {
                }

                @Override
                public void unregister(HealthChecker healthChecker) {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            });
        ObjectMapper mapper = new ObjectMapper();
        File amzaDataDir = Files.createTempDir();
        MiruAmzaServiceConfig acrc = BindInterfaceToConfiguration.bindDefault(MiruAmzaServiceConfig.class);
        acrc.setWorkingDirectories(amzaDataDir.getAbsolutePath());
        Deployable deployable = new Deployable(new String[0]);
        AmzaService amzaService = new MiruAmzaServiceInitializer().initialize(deployable,
            connectionDescriptor -> new NoOpClientHealth(),
            1,
            "instanceKey",
            "serviceName",
            "datacenter",
            "rack",
            "localhost",
            10000,
            false,
            null,
            acrc,
            false,
            1,
            rowsChanged -> {
            });
        registry = new AmzaClusterRegistry(amzaService,
            new EmbeddedClientProvider(amzaService),
            10_000L,
            new JacksonJsonObjectTypeMarshaller<>(MiruSchema.class, mapper),
            3,
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.DAYS.toMillis(365));

        replicaSetDirector = new MiruReplicaSetDirector(new OrderIdProviderImpl(new ConstantWriterIdProvider(1)), registry,
            stream -> {
                for (MiruHost host : hosts) {
                    stream.descriptor("datacenter", "rack", host);
                }
            }, false);
    }

    @Test
    public void testUpdateAndGetTopology() throws Exception {
        hosts = addHosts(1);

        Set<MiruHost> electedHosts = replicaSetDirector.electHostsForTenantPartition(tenantId,
            partitionId,
            new MiruReplicaSet(ArrayListMultimap.create(), Sets.<MiruHost>newHashSet(), numReplicas, numReplicas));
        assertEquals(electedHosts.size(), 1);

        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, hosts.get(0));
        registry.updateIngress(new MiruIngressUpdate(tenantId, partitionId, new RangeMinMax(), System.currentTimeMillis(), false));
        registry.updateTopologies(hosts.get(0), Arrays.asList(
            new MiruClusterRegistry.TopologyUpdate(coord,
                Optional.of(new MiruPartitionCoordInfo(MiruPartitionState.online, MiruBackingStorage.disk)),
                Optional.<Long>absent())));

        List<MiruTopologyStatus> topologyStatusForTenantHost = registry.getTopologyStatusForTenantHost(tenantId, hosts.get(0));
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
        hosts = addHosts(1);

        Set<MiruHost> electedHosts = replicaSetDirector.electHostsForTenantPartition(tenantId,
            partitionId,
            new MiruReplicaSet(ArrayListMultimap.create(), Sets.<MiruHost>newHashSet(), numReplicas, numReplicas));
        assertEquals(electedHosts.size(), 1);

        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, partitionId, hosts.get(0));
        registry.updateIngress(new MiruIngressUpdate(tenantId, partitionId, new RangeMinMax(), System.currentTimeMillis(), false));

        List<MiruTopologyStatus> topologyStatusForTenantHost = registry.getTopologyStatusForTenantHost(tenantId, hosts.get(0));
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
        hosts = addHosts(4);

        Set<MiruHost> electedHosts = replicaSetDirector.electHostsForTenantPartition(tenantId,
            partitionId,
            new MiruReplicaSet(ArrayListMultimap.create(), Sets.<MiruHost>newHashSet(), numReplicas, numReplicas));

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

    @Test
    public void testSchemaProvider() throws Exception {
        MiruTenantId tenantId1 = new MiruTenantId("tenant1".getBytes());
        MiruSchema schema1 = new MiruSchema.Builder("test1", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
                new MiruFieldDefinition(1, "b", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE)
            })
            .build();
        MiruTenantId tenantId2 = new MiruTenantId("tenant2".getBytes());
        MiruSchema schema2 = new MiruSchema.Builder("test2", 2)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "c", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
                new MiruFieldDefinition(1, "d", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE)
            })
            .build();

        InMemoryRowColumnValueStore<MiruVoidByte, MiruTenantId, MiruSchemaColumnKey, MiruSchema> schemaRegistry = new InMemoryRowColumnValueStore<>();

        registry.registerSchema(tenantId1, schema1);
        registry.registerSchema(tenantId2, schema2);

        assertEquals(registry.getSchema(tenantId1).getName(), "test1");
        assertEquals(registry.getSchema(tenantId1).getVersion(), 1L);
        assertEquals(registry.getSchema(tenantId1).fieldCount(), 2);
        assertEquals(registry.getSchema(tenantId1).getFieldDefinition(0).name, "a");
        assertEquals(registry.getSchema(tenantId1).getFieldDefinition(1).name, "b");

        assertEquals(registry.getSchema(tenantId2).getName(), "test2");
        assertEquals(registry.getSchema(tenantId2).getVersion(), 2L);
        assertEquals(registry.getSchema(tenantId2).fieldCount(), 2);
        assertEquals(registry.getSchema(tenantId2).getFieldDefinition(0).name, "c");
        assertEquals(registry.getSchema(tenantId2).getFieldDefinition(1).name, "d");
    }

    @Test
    public void testSchemaVersions() throws Exception {
        MiruTenantId tenantId1 = new MiruTenantId("tenant1".getBytes());
        MiruSchema schema1 = new MiruSchema.Builder("test1", 1)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "a", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
                new MiruFieldDefinition(1, "b", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE)
            })
            .build();
        MiruSchema schema2 = new MiruSchema.Builder("test1", 2)
            .setFieldDefinitions(new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "c", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
                new MiruFieldDefinition(1, "d", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE)
            })
            .build();

        registry.registerSchema(tenantId1, schema1);

        assertEquals(registry.getSchema(tenantId1).getName(), "test1");
        assertEquals(registry.getSchema(tenantId1).getVersion(), 1L);
        assertEquals(registry.getSchema(tenantId1).fieldCount(), 2);
        assertEquals(registry.getSchema(tenantId1).getFieldDefinition(0).name, "a");
        assertEquals(registry.getSchema(tenantId1).getFieldDefinition(1).name, "b");

        registry.registerSchema(tenantId1, schema2);

        assertEquals(registry.getSchema(tenantId1).getName(), "test1");
        assertEquals(registry.getSchema(tenantId1).getVersion(), 2L);
        assertEquals(registry.getSchema(tenantId1).fieldCount(), 2);
        assertEquals(registry.getSchema(tenantId1).getFieldDefinition(0).name, "c");
        assertEquals(registry.getSchema(tenantId1).getFieldDefinition(1).name, "d");
    }

    private List<MiruHost> addHosts(int numHosts) throws Exception {
        List<MiruHost> hosts = Lists.newArrayListWithCapacity(numHosts);
        for (int i = 0; i < numHosts; i++) {
            MiruHost host = MiruHostProvider.fromInstance(i, "instanceKey" + i);
            hosts.add(host);
            registry.heartbeat(host);
        }
        return hosts;
    }
}
