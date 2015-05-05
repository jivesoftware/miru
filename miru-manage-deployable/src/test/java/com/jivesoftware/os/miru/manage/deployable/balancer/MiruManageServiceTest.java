package com.jivesoftware.os.miru.manage.deployable.balancer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryClusterClient;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.MiruRegistryStoreInitializer;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.manage.deployable.MiruManageInitializer;
import com.jivesoftware.os.miru.manage.deployable.MiruManageService;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRenderer;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRendererInitializer;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.rcvs.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.rcvs.inmemory.InMemoryRowColumnValueStoreInitializer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.merlin.config.BindInterfaceToConfiguration;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class MiruManageServiceTest {

    private MiruManageService miruManageService;
    private MiruTenantId tenantId;
    private List<MiruHost> hosts;
    private MiruPartitionId partitionId;

    private MiruSchema miruSchema = new MiruSchema.Builder("test", 1)
        .setFieldDefinitions(new MiruFieldDefinition[]{
            new MiruFieldDefinition(0, "user", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE),
            new MiruFieldDefinition(1, "doc", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE)
        })
        .setPairedLatest(ImmutableMap.of(
                "user", Arrays.asList("doc"),
                "doc", Arrays.asList("user")))
        .setBloom(ImmutableMap.of(
                "doc", Arrays.asList("user")))
        .build();

    @BeforeClass
    public void before() throws Exception {
        int numberOfReplicas = 3;

        tenantId = new MiruTenantId("test1".getBytes());
        partitionId = MiruPartitionId.of(0);
        hosts = Lists.newArrayList();

        MiruSoyRendererConfig config = BindInterfaceToConfiguration.bindDefault(MiruSoyRendererConfig.class);
        config.setPathToSoyResources("src/main/home/resources/soy");

        InMemoryRowColumnValueStoreInitializer rowColumnValueStoreInitializer = new InMemoryRowColumnValueStoreInitializer();
        ObjectMapper mapper = new ObjectMapper();
        MiruRegistryStore registryStore = new MiruRegistryStoreInitializer().initialize("test", rowColumnValueStoreInitializer, mapper);
        MiruClusterRegistry clusterRegistry = new MiruRCVSClusterRegistry(new CurrentTimestamper(),
            registryStore.getHostsRegistry(),
            registryStore.getExpectedTenantsRegistry(),
            registryStore.getTopologyUpdatesRegistry(),
            registryStore.getExpectedTenantPartitionsRegistry(),
            registryStore.getReplicaRegistry(),
            registryStore.getTopologyRegistry(),
            registryStore.getConfigRegistry(),
            registryStore.getSchemaRegistry(),
            numberOfReplicas,
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.HOURS.toMillis(1));

        clusterRegistry.registerSchema(tenantId, miruSchema);

        MiruWALClient miruWALClient = Mockito.mock(MiruWALClient.class);
        Mockito.when(miruWALClient.getAllTenantIds()).thenReturn(Arrays.asList(tenantId));
        Mockito.when(miruWALClient.getLargestPartitionIdAcrossAllWriters(Mockito.<MiruTenantId>any())).thenReturn(partitionId);

        Mockito.when(miruWALClient.getPartitionStatus(Mockito.<MiruTenantId>any(), Mockito.anyList()))
            .thenReturn(Arrays.asList(new MiruActivityWALStatus(partitionId, 10, Arrays.asList(0), Arrays.asList(0))));

        MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(config);
        MiruStats stats = new MiruStats();

        miruManageService = new MiruManageInitializer().initialize(renderer,
            clusterRegistry,
            miruWALClient,
            stats);

        MiruRegistryClusterClient clusterClient = new MiruRegistryClusterClient(clusterRegistry);

        long electTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        for (int i = 0; i < numberOfReplicas; i++) {
            MiruHost host = new MiruHost("host" + i, 10_000 + i);
            clusterRegistry.sendHeartbeatForHost(host);
            hosts.add(host);
            clusterClient.elect(host, tenantId,
                partitionId,
                electTime - i);
        }

    }

    @Test
    public void testRenderHostsWithFocus() throws Exception {
        String rendered = miruManageService.renderHostsWithFocus(hosts.get(0));
        for (int i = 0; i < hosts.size(); i++) {
            MiruHost host = hosts.get(i);
            assertTrue(rendered.contains("/miru/manage/hosts/" + host.getLogicalName() + "/" + host.getPort() + "#focus"));
            assertTrue(rendered.contains("<td>" + host.getPort() + "</td>"));
            if (i == 0) {
                assertTrue(rendered.contains(host.toStringForm()));
            } else {
                assertFalse(rendered.contains(host.toStringForm()));
            }
        }
    }

    @Test
    public void testRenderTenantsWithFocus() throws Exception {
        String rendered = miruManageService.renderTenantsWithFocus(tenantId);
        assertTrue(rendered.contains(tenantId.toString()));
        for (MiruHost host : hosts) {
            assertTrue(rendered.contains("/miru/manage/hosts/" + host.getLogicalName() + "/" + host.getPort() + "#focus"));
        }
    }

    @Test
    public void testRenderSchemaWithLookup() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        String lookupJSON = objectMapper.writeValueAsString(miruSchema);
        String rendered = miruManageService.renderSchemaWithLookup(lookupJSON);
        System.out.println(rendered);
        assertTrue(rendered.contains("<td>" + tenantId.toString() + "</td>"));
    }

}
