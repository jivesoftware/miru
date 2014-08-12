package com.jivesoftware.os.miru.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfig;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.cluster.MiruRegistryInitializer;
import com.jivesoftware.os.miru.cluster.MiruRegistryInitializerModule;
import com.jivesoftware.os.miru.reader.MiruHttpClientReaderConfig;
import com.jivesoftware.os.miru.service.partition.MiruExpectedTenants;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartitionComparison;
import com.jivesoftware.os.miru.service.schema.MiruSchema;
import com.jivesoftware.os.miru.service.stream.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.stream.locator.MiruTransientResourceLocator;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.miru.wal.MiruWALInitializerModule;
import java.io.File;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Singleton;

import static com.google.common.base.Preconditions.checkNotNull;

public class MiruServiceInitializerModule extends AbstractModule {

    private final MiruServiceConfig config;
    private final MiruHost miruHost;
    private final MiruSchema miruSchema;
    private final MiruRegistryInitializer miruRegistryInitializer;
    private final MiruWALInitializer miruWALInitializer;
    private final MiruHttpClientReaderConfig miruHttpClientReaderConfig;

    public MiruServiceInitializerModule(MiruServiceConfig config,
        MiruHost miruHost,
        MiruSchema miruSchema,
        MiruRegistryInitializer miruRegistryInitializer,
        MiruWALInitializer miruWALInitializer,
        MiruHttpClientReaderConfig miruHttpClientReaderConfig) {

        this.miruRegistryInitializer = miruRegistryInitializer;
        this.miruWALInitializer = miruWALInitializer;
        this.config = checkNotNull(config);
        this.miruHost = checkNotNull(miruHost);
        this.miruSchema = checkNotNull(miruSchema);
        this.miruHttpClientReaderConfig = checkNotNull(miruHttpClientReaderConfig);
    }

    @Override
    protected void configure() {
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

        // disk resource locator
        bindConstant().annotatedWith(Names.named("miruDiskResourceLocatorPath"))
            .to(new File(config.getDiskResourceLocatorPath(), miruHost.toStringForm()).getAbsolutePath());
        bindConstant().annotatedWith(Names.named("miruDiskResourceInitialChunkSize")).to(config.getDiskResourceInitialChunkSize());
        bind(MiruResourceLocator.class)
            .annotatedWith(Names.named("miruDiskResourceLocator"))
            .to(config.getDiskResourceLocatorClass());

        // transient resource locator
        bindConstant().annotatedWith(Names.named("miruTransientResourceLocatorPath"))
            .to(new File(config.getTransientResourceLocatorPath(), miruHost.toStringForm()).getAbsolutePath());
        bindConstant().annotatedWith(Names.named("miruTransientResourceInitialChunkSize")).to(config.getTransientResourceInitialChunkSize());
        bind(MiruTransientResourceLocator.class)
            .annotatedWith(Names.named("miruTransientResourceLocator"))
            .to(config.getTransientResourceLocatorClass());

        bind(MiruHost.class)
            .annotatedWith(Names.named("miruServiceHost"))
            .toInstance(miruHost);
        bind(MiruSchema.class).toInstance(miruSchema);
        bind(MiruHostedPartitionComparison.class).toInstance(
            new MiruHostedPartitionComparison(config.getLongTailSolverWindowSize(), config.getLongTailSolverPercentile()));

        Collection<HttpClientConfiguration> configurations = Lists.newArrayList();
        HttpClientConfig baseConfig = HttpClientConfig.newBuilder()
            .setSocketTimeoutInMillis(miruHttpClientReaderConfig.getSocketTimeoutInMillis())
            .setMaxConnections(miruHttpClientReaderConfig.getMaxConnections())
            .build();
        configurations.add(baseConfig);
        bind(HttpClientFactory.class).toInstance(new HttpClientFactoryProvider().createHttpClientFactory(configurations));

        install(new MiruRegistryInitializerModule(
            miruRegistryInitializer.getConfig(),
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