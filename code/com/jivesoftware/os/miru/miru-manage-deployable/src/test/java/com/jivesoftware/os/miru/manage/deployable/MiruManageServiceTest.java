package com.jivesoftware.os.miru.manage.deployable;

import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.InMemorySetOfSortedMapsImplInitializer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.MiruRegistryStoreInitializer;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import java.util.concurrent.TimeUnit;
import org.merlin.config.BindInterfaceToConfiguration;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.manage.deployable.MiruManageInitializer.MiruManageConfig;
import static com.jivesoftware.os.miru.wal.MiruWALInitializer.MiruWAL;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class MiruManageServiceTest {

    @Test
    public void testRenderHostsWithFocus() throws Exception {
        MiruManageConfig config = BindInterfaceToConfiguration.bindDefault(MiruManageConfig.class);
        config.setPathToSoyResources("src/main/home/resources/soy");

        InMemorySetOfSortedMapsImplInitializer setOfSortedMapsImplInitializer = new InMemorySetOfSortedMapsImplInitializer();
        MiruRegistryStore registryStore = new MiruRegistryStoreInitializer().initialize("test", setOfSortedMapsImplInitializer);
        MiruClusterRegistry clusterRegistry = new MiruRCVSClusterRegistry(registryStore.getHostsRegistry(), registryStore.getExpectedTenantsRegistry(),
                registryStore.getExpectedTenantPartitionsRegistry(), registryStore.getReplicaRegistry(), registryStore.getTopologyRegistry(),
                registryStore.getConfigRegistry(), 3, TimeUnit.HOURS.toMillis(1));
        MiruWAL miruWAL = new MiruWALInitializer().initialize("test", setOfSortedMapsImplInitializer);
        MiruManageService miruManageService = new MiruManageInitializer().initialize(config, clusterRegistry, registryStore, miruWAL);

        MiruHost host1 = new MiruHost("host1", 10001);
        MiruHost host2 = new MiruHost("host2", 10002);
        clusterRegistry.sendHeartbeatForHost(host1, 123, 456);
        clusterRegistry.sendHeartbeatForHost(host2, 789, 987);

        String rendered = miruManageService.renderHostsWithFocus(host1);
        assertTrue(rendered.contains("/miru/manage/hosts/host1/10001#focus"));
        assertTrue(rendered.contains("<td>10001</td>"));
        assertTrue(rendered.contains("/miru/manage/hosts/host2/10002#focus"));
        assertTrue(rendered.contains("<td>10002</td>"));
        assertTrue(rendered.contains("host1:10001"));
        assertFalse(rendered.contains("host2:10002"));
    }
}