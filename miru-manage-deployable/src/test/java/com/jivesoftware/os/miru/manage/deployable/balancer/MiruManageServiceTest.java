package com.jivesoftware.os.miru.manage.deployable.balancer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.client.AmzaKretrProvider;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceConfig;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceInitializer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
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
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import java.io.File;
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
        File amzaIndexDir = Files.createTempDir();
        MiruAmzaServiceConfig acrc = BindInterfaceToConfiguration.bindDefault(MiruAmzaServiceConfig.class);
        acrc.setWorkingDirectories(amzaDataDir.getAbsolutePath());
        acrc.setIndexDirectories(amzaIndexDir.getAbsolutePath());
        Deployable deployable = new Deployable(new String[0]);
        AmzaService amzaService = new MiruAmzaServiceInitializer().initialize(deployable, 1, "instanceKey", "localhost", 10000, "test-cluster", acrc,
            rowsChanged -> {
            });
        MiruClusterRegistry clusterRegistry = new AmzaClusterRegistry(amzaService,
            new AmzaKretrProvider(amzaService),
            0,
            10_000L,
            new JacksonJsonObjectTypeMarshaller<>(MiruSchema.class, mapper),
            3,
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.DAYS.toMillis(365),
            0,
            0);

        clusterRegistry.registerSchema(tenantId, miruSchema);

        MiruWALClient miruWALClient = Mockito.mock(MiruWALClient.class);
        Mockito.when(miruWALClient.getAllTenantIds()).thenReturn(Arrays.asList(tenantId));
        Mockito.when(miruWALClient.getLargestPartitionId(Mockito.<MiruTenantId>any())).thenReturn(partitionId);

        Mockito.when(miruWALClient.getPartitionStatus(Mockito.<MiruTenantId>any(), Mockito.any(MiruPartitionId.class)))
            .thenReturn(new MiruActivityWALStatus(partitionId, 10, Arrays.asList(0), Arrays.asList(0)));

        MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(config);
        MiruStats stats = new MiruStats();

        miruManageService = new MiruManageInitializer().initialize(renderer,
            clusterRegistry,
            miruWALClient,
            stats);

        OrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1), new SnowflakeIdPacker(), new JiveEpochTimestampProvider());

        long electTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        for (int i = 0; i < numberOfReplicas; i++) {
            MiruHost host = new MiruHost("host" + i, 10_000 + i);
            clusterRegistry.heartbeat(host);
            hosts.add(host);
            clusterRegistry.addToReplicaRegistry(tenantId,
                partitionId,
                electTime - i,
                host);
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
