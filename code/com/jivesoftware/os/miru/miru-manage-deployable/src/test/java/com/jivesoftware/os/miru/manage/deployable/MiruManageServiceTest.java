package com.jivesoftware.os.miru.manage.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.marshall.MiruVoidByte;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryClusterClient;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.MiruRegistryStoreInitializer;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.miru.wal.MiruWALInitializer.MiruWAL;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReaderImpl;
import com.jivesoftware.os.miru.wal.lookup.MiruActivityLookupTable;
import com.jivesoftware.os.miru.wal.lookup.MiruRCVSActivityLookupTable;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import com.jivesoftware.os.rcvs.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.rcvs.inmemory.InMemoryRowColumnValueStoreInitializer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.merlin.config.BindInterfaceToConfiguration;
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
            registryStore.getExpectedTenantPartitionsRegistry(),
            registryStore.getReplicaRegistry(),
            registryStore.getTopologyRegistry(),
            registryStore.getConfigRegistry(),
            registryStore.getSchemaRegistry(),
            numberOfReplicas,
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.HOURS.toMillis(1));
        MiruWAL miruWAL = new MiruWALInitializer().initialize("test", rowColumnValueStoreInitializer, mapper);
        miruWAL.getWriterPartitionRegistry().add(MiruVoidByte.INSTANCE, tenantId, 1, MiruPartitionId.of(0), null, null);

        MiruActivityWALReader activityWALReader = new MiruActivityWALReaderImpl(miruWAL.getActivityWAL(), miruWAL.getActivitySipWAL(),
            miruWAL.getWriterPartitionRegistry());
        MiruReadTrackingWALReader readTrackingWALReader = new MiruReadTrackingWALReaderImpl(miruWAL.getReadTrackingWAL(), miruWAL.getReadTrackingSipWAL());
        MiruActivityLookupTable activityLookupTable = new MiruRCVSActivityLookupTable(miruWAL.getActivityLookupTable());

        clusterRegistry.registerSchema(tenantId, miruSchema);

        MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(config);
        miruManageService = new MiruManageInitializer().initialize(renderer, clusterRegistry, activityWALReader, readTrackingWALReader,
            activityLookupTable);

        MiruRegistryClusterClient clusterClient = new MiruRegistryClusterClient(clusterRegistry);

        for (int i = 0; i < numberOfReplicas; i++) {
            MiruHost host = new MiruHost("host" + i, 10_000 + i);
            clusterRegistry.sendHeartbeatForHost(host);
            hosts.add(host);
            clusterClient.elect(host, tenantId,
                partitionId,
                System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
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

    @Test
    public void testRenderActivityWALWithTenant() throws Exception {
        String rendered = miruManageService.renderActivityWALWithTenant(tenantId);
        assertTrue(rendered.contains(tenantId.toString()));
        assertTrue(rendered.contains("/miru/manage/wal/activity/" + tenantId + "/" + partitionId + "#focus"));
    }

}
