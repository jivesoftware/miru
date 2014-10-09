package com.jivesoftware.os.miru.plugin.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Interners;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.api.MiruPartition;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.MiruRegistryStoreInitializer;
import com.jivesoftware.os.miru.cluster.MiruReplicaSet;
import com.jivesoftware.os.miru.cluster.MiruReplicaSetDirector;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.SingleBitmapsProvider;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruJustInTimeBackfillerizer;
import com.jivesoftware.os.miru.plugin.schema.SingleSchemaProvider;
import com.jivesoftware.os.miru.service.MiruBackfillerizerInitializer;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.MiruServiceInitializer;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocatorProvider;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.rcvs.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.rcvs.inmemory.InMemorySetOfSortedMapsImplInitializer;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import org.merlin.config.BindInterfaceToConfiguration;
import org.testng.Assert;

/**
 *
 */
public class MiruPluginTestBootstrap {

    public <BM> MiruProvider<MiruService> bootstrap(MiruTenantId tenantId,
            MiruPartitionId partitionId,
            MiruHost miruHost,
            MiruSchema miruSchema,
            MiruBackingStorage desiredStorage,
            final MiruBitmaps<BM> bitmaps)
            throws Exception {

        MiruServiceConfig config = BindInterfaceToConfiguration.bindDefault(MiruServiceConfig.class);
        config.setDefaultStorage(desiredStorage.name());
        config.setDefaultFailAfterNMillis(TimeUnit.HOURS.toMillis(1));

        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
                .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList());

        ObjectMapper mapper = new ObjectMapper();

        InMemorySetOfSortedMapsImplInitializer inMemorySetOfSortedMapsImplInitializer = new InMemorySetOfSortedMapsImplInitializer();
        MiruRegistryStore registryStore = new MiruRegistryStoreInitializer().initialize("test", inMemorySetOfSortedMapsImplInitializer, mapper);
        MiruClusterRegistry clusterRegistry = new MiruRCVSClusterRegistry(
            new CurrentTimestamper(),
                registryStore.getHostsRegistry(),
                registryStore.getExpectedTenantsRegistry(),
                registryStore.getExpectedTenantPartitionsRegistry(),
                registryStore.getReplicaRegistry(),
                registryStore.getTopologyRegistry(),
                registryStore.getConfigRegistry(),
                3,
                TimeUnit.HOURS.toMillis(1));

        MiruReplicaSetDirector replicaSetDirector = new MiruReplicaSetDirector(new OrderIdProviderImpl(new ConstantWriterIdProvider(1)), clusterRegistry);

        clusterRegistry.sendHeartbeatForHost(miruHost, 0, 0);
        replicaSetDirector.electToReplicaSetForTenantPartition(tenantId, partitionId,
                new MiruReplicaSet(ArrayListMultimap.<MiruPartitionState, MiruPartition>create(), new HashSet<MiruHost>(), 3),
                System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));

        MiruWALInitializer.MiruWAL wal = new MiruWALInitializer().initialize("test", inMemorySetOfSortedMapsImplInitializer, mapper);


        MiruLifecyle<MiruJustInTimeBackfillerizer> backfillerizerLifecycle = new MiruBackfillerizerInitializer().initialize(config, miruHost);

        backfillerizerLifecycle.start();
        final MiruJustInTimeBackfillerizer backfillerizer = backfillerizerLifecycle.getService();

        MiruLifecyle<MiruResourceLocatorProvider> miruResourceLocatorProviderLifecyle = new MiruTempResourceLocatorProviderInitializer().initialize();
        miruResourceLocatorProviderLifecyle.start();
        final MiruActivityInternExtern activityInternExtern = new MiruActivityInternExtern(Interners.<MiruIBA>newWeakInterner(),
                Interners.<MiruTermId>newWeakInterner(), Interners.<MiruTenantId>newWeakInterner(), Interners.<String>newWeakInterner());
        MiruLifecyle<MiruService> miruServiceLifecyle = new MiruServiceInitializer().initialize(config,
                registryStore,
                clusterRegistry,
                miruHost,
                new SingleSchemaProvider(miruSchema),
                wal,
                httpClientFactory,
                miruResourceLocatorProviderLifecyle.getService(),
                activityInternExtern,
                new SingleBitmapsProvider(bitmaps));

        miruServiceLifecyle.start();
        final MiruService miruService = miruServiceLifecyle.getService();

        long t = System.currentTimeMillis();
        while (!miruService.checkInfo(tenantId, partitionId, new MiruPartitionCoordInfo(MiruPartitionState.online, desiredStorage))) {
            Thread.sleep(10);
            if (System.currentTimeMillis() - t > TimeUnit.SECONDS.toMillis(5000)) {
                Assert.fail("Partition failed to come online");
            }
        }

        return new MiruProvider<MiruService>() {
            @Override
            public MiruService getMiru(MiruTenantId tenantId) {
                return miruService;
            }

            @Override
            public MiruActivityInternExtern getActivityInternExtern(MiruTenantId tenantId) {
                return activityInternExtern;
            }

            @Override
            public MiruJustInTimeBackfillerizer getBackfillerizer(MiruTenantId tenantId) {
                return backfillerizer;
            }
        };
    }
}
