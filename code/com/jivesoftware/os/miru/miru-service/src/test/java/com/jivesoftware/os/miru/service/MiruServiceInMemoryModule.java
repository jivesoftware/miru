package com.jivesoftware.os.miru.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfig;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.InMemorySetOfSortedMapsImplInitializer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.cluster.MiruRegistryConfig;
import com.jivesoftware.os.miru.cluster.MiruRegistryInitializer;
import com.jivesoftware.os.miru.cluster.MiruRegistryInitializerModule;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.service.partition.MiruExpectedTenants;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartitionComparison;
import com.jivesoftware.os.miru.service.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.service.schema.MiruSchema;
import com.jivesoftware.os.miru.service.stream.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.stream.locator.MiruTransientResourceLocator;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.miru.wal.MiruWALInitializerModule;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReaderImpl;
import com.jivesoftware.os.miru.wal.activity.MiruWriteToActivityAndSipWAL;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import com.jivesoftware.os.miru.wal.readtracking.MiruWriteToReadTrackingAndSipWAL;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Singleton;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MiruServiceInMemoryModule extends AbstractModule {

    @Override
    protected void configure() {
        MiruHost miruHost = new MiruHost("localhost", 49608);
        MiruServiceConfig config = new MockMiruServiceConfig();

        MiruRegistryConfig registryConfig = mock(MiruRegistryConfig.class);
        when(registryConfig.getDefaultNumberOfReplicas()).thenReturn(3);
        when(registryConfig.getDefaultTopologyIsStaleAfterMillis()).thenReturn(1_000_000L);

        SetOfSortedMapsImplInitializer<? extends Exception> setOfSortedMapsInitializer = new InMemorySetOfSortedMapsImplInitializer();
        MiruRegistryInitializer miruRegistryInitializer;
        MiruWALInitializer miruWALInitializer;
        try {
            miruRegistryInitializer = MiruRegistryInitializer.initialize(registryConfig,
                "memory",
                setOfSortedMapsInitializer,
                MiruRCVSClusterRegistry.class);

            miruWALInitializer = MiruWALInitializer.initialize("memory",
                setOfSortedMapsInitializer,
                MiruActivityWALReaderImpl.class,
                MiruWriteToActivityAndSipWAL.class,
                MiruReadTrackingWALReaderImpl.class,
                MiruWriteToReadTrackingAndSipWAL.class);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

        bind(MiruServiceConfig.class).toInstance(config);
        bind(Executor.class)
            .annotatedWith(Names.named("miruServiceExecutor"))
            .toInstance(Executors.newFixedThreadPool(10)); // TODO - config?
        bind(ScheduledExecutorService.class)
            .annotatedWith(Names.named("miruScheduledExecutor"))
            .toInstance(Executors.newScheduledThreadPool(6)); // TODO - config?
        bind(ExecutorService.class)
            .annotatedWith(Names.named("miruBackfillExecutor"))
            .toInstance(Executors.newFixedThreadPool(10)); // TODO - config? should be at least as big as miruServiceExecutor!
        bind(ExecutorService.class)
            .annotatedWith(Names.named("miruStreamFactoryExecutorService"))
            .toInstance(Executors.newFixedThreadPool(config.getStreamFactoryExecutorCount()));

        bind(MiruExpectedTenants.class).to(config.getExpectedTenantsClass());

        bindConstant().annotatedWith(Names.named("miruDiskResourceLocatorPath")).to(config.getDiskResourceLocatorPath());
        bindConstant().annotatedWith(Names.named("miruDiskResourceInitialChunkSize")).to(config.getDiskResourceInitialChunkSize());
        bind(MiruResourceLocator.class)
            .annotatedWith(Names.named("miruDiskResourceLocator"))
            .to(config.getDiskResourceLocatorClass());

        bindConstant().annotatedWith(Names.named("miruTransientResourceLocatorPath")).to(config.getTransientResourceLocatorPath());
        bindConstant().annotatedWith(Names.named("miruTransientResourceInitialChunkSize")).to(config.getTransientResourceInitialChunkSize());
        bind(MiruTransientResourceLocator.class)
            .annotatedWith(Names.named("miruTransientResourceLocator"))
            .to(config.getTransientResourceLocatorClass());

        bind(MiruHost.class)
            .annotatedWith(Names.named("miruServiceHost"))
            .toInstance(miruHost);
        bind(MiruSchema.class).toInstance(new MiruSchema(DefaultMiruSchemaDefinition.SCHEMA));
        bind(MiruHostedPartitionComparison.class).toInstance(new MiruHostedPartitionComparison(100, 95));

        Collection<HttpClientConfiguration> configurations = Lists.newArrayList();
        HttpClientConfig baseConfig = HttpClientConfig.newBuilder()
            .setSocketTimeoutInMillis(5000)
            .setMaxConnections(100)
            .build();
        configurations.add(baseConfig);
        bind(HttpClientFactory.class).toInstance(new HttpClientFactoryProvider().createHttpClientFactory(configurations));

        install(new MiruRegistryInitializerModule(
            registryConfig,
            miruRegistryInitializer.getHostsRegistry(),
            miruRegistryInitializer.getExpectedTenantsRegistry(),
            miruRegistryInitializer.getExpectedTenantPartitionsRegistry(),
            miruRegistryInitializer.getReplicaRegistry(),
            miruRegistryInitializer.getTopologyRegistry(),
            miruRegistryInitializer.getConfigRegistry(),
            miruRegistryInitializer.getWriterPartitionRegistry(),
            miruRegistryInitializer.getActivityLookupTable(),
            config.getClusterRegistryClass()));

        install(new MiruWALInitializerModule(
            miruWALInitializer.getActivityWAL(), miruWALInitializer.getActivitySipWAL(),
            miruWALInitializer.getReadTrackingWAL(), miruWALInitializer.getReadTrackingSipWAL(),
            config.getActivityWALReaderClass(), config.getActivityWALWriterClass(),
            config.getReadTrackingWALReaderClass(), config.getReadTrackingWALWriterClass()));
    }

    @Singleton
    @Provides
    public ObjectMapper provideObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());
        return mapper;
    }
}
