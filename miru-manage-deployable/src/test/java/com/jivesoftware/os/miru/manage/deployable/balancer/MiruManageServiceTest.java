package com.jivesoftware.os.miru.manage.deployable.balancer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceConfig;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceInitializer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.api.wal.MiruActivityWALStatus;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.amza.AmzaClusterRegistry;
import com.jivesoftware.os.miru.manage.deployable.MiruManageInitializer;
import com.jivesoftware.os.miru.manage.deployable.MiruManageService;
import com.jivesoftware.os.miru.manage.deployable.region.MiruSchemaRegion;
import com.jivesoftware.os.miru.amza.NoOpClientHealth;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
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
        .setFieldDefinitions(new MiruFieldDefinition[] {
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
            rowsChanged -> {
            });
        MiruClusterRegistry clusterRegistry = new AmzaClusterRegistry(amzaService,
            new EmbeddedClientProvider(amzaService),
            10_000L,
            new JacksonJsonObjectTypeMarshaller<>(MiruSchema.class, mapper),
            3,
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.DAYS.toMillis(365));

        clusterRegistry.registerSchema(tenantId, miruSchema);

        MiruWALClient miruWALClient = Mockito.mock(MiruWALClient.class);
        Mockito.when(miruWALClient.getAllTenantIds()).thenReturn(Collections.singletonList(tenantId));
        Mockito.when(miruWALClient.getLargestPartitionId(Mockito.<MiruTenantId>any())).thenReturn(partitionId);

        Mockito.when(miruWALClient.getActivityWALStatusForTenant(Mockito.<MiruTenantId>any(), Mockito.any(MiruPartitionId.class)))
            .thenReturn(new MiruActivityWALStatus(partitionId,
                Collections.singletonList(new MiruActivityWALStatus.WriterCount(1, 10)),
                Collections.singletonList(0),
                Collections.singletonList(0)));

        MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(config);
        MiruStats stats = new MiruStats();

        miruManageService = new MiruManageInitializer().initialize("test",
            1,
            renderer,
            clusterRegistry,
            miruWALClient,
            stats,
            null,
            mapper);

        long electTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        for (int i = 0; i < numberOfReplicas; i++) {
            MiruHost host = new MiruHost("logicalName_" + i);
            clusterRegistry.heartbeat(host);
            hosts.add(host);

            clusterRegistry.addToReplicaRegistry(
                ImmutableListMultimap.<MiruHost, TenantAndPartition>builder().put(host, new TenantAndPartition(tenantId, partitionId)).build(),
                electTime - i);
        }

    }

    @Test
    public void testRenderHostsWithFocus() throws Exception {
        String rendered = miruManageService.renderHostsWithFocus(hosts.get(0));
        for (int i = 0; i < hosts.size(); i++) {
            MiruHost host = hosts.get(i);
            assertTrue(rendered.contains("/ui/hosts/" + host.getLogicalName() + "#focus"));
            if (i == 0) {
                assertTrue(rendered.contains("Expected Tenants for " + host.getLogicalName()));
            } else {
                assertFalse(rendered.contains("Expected Tenants for " + host.getLogicalName()));
            }
        }
    }

    @Test
    public void testRenderTenantsWithFocus() throws Exception {
        String rendered = miruManageService.renderTenantsWithFocus(tenantId);
        assertTrue(rendered.contains(tenantId.toString()));
        for (MiruHost host : hosts) {
            assertTrue(rendered.contains("/ui/hosts/" + host.getLogicalName() + "#focus"));
        }
    }

    @Test
    public void testRenderSchemaWithLookup() throws Exception {
        String rendered = miruManageService.renderSchema(new MiruSchemaRegion.SchemaInput(null, "test", 1, "lookup", false, false));
        //System.out.println(rendered);
        assertTrue(rendered.contains("<tr><td>1</td><td>0</td><td>0</td></tr>"));
    }
}
