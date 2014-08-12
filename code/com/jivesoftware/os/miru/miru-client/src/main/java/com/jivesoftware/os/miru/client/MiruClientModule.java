package com.jivesoftware.os.miru.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.client.base.MiruHttpActivitySenderProvider;
import com.jivesoftware.os.miru.client.rcvs.MiruRCVSPartitionIdProvider;
import com.jivesoftware.os.miru.cluster.MiruClusterModule;
import com.jivesoftware.os.miru.cluster.MiruRegistryInitializer;
import com.jivesoftware.os.miru.cluster.MiruRegistryInitializerModule;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.miru.wal.MiruWALInitializerModule;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;
import javax.inject.Singleton;

public class MiruClientModule extends AbstractModule {

    private final MiruClientConfig config;
    private final MiruRegistryInitializer miruRegistryInitializer;
    private final MiruWALInitializer miruWALInitializer;
    private final int writerId;
    private final Optional<Class<? extends MiruClient>> clientClass;

    public MiruClientModule(MiruClientConfig config,
        MiruRegistryInitializer miruRegistryInitializer,
        MiruWALInitializer miruWALInitializer,
        int writerId,
        Optional<Class<? extends MiruClient>> clientClass) {

        this.config = config;
        this.miruRegistryInitializer = miruRegistryInitializer;
        this.miruWALInitializer = miruWALInitializer;
        this.writerId = writerId;
        this.clientClass = clientClass;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure() {
        bind(MiruClientConfig.class).toInstance(config);

        // required by the naive client
        bind(MiruRegistryInitializer.class).toInstance(miruRegistryInitializer);
        bind(MiruWALInitializer.class).toInstance(miruWALInitializer);

        bindConstant()
            .annotatedWith(Names.named("miruWriterId"))
            .to(writerId);
        bind(ExecutorService.class)
            .annotatedWith(Names.named("sendActivitiesThreadPool"))
            .toInstance(Executors.newFixedThreadPool(config.getSendActivitiesThreadPoolSize()));
        bind(new TypeLiteral<List<MiruHost>>() {
        }).annotatedWith(Names.named("miruDefaultHosts")).toInstance(extractDefaultHosts(config));
        bind(MiruActivitySenderProvider.class).to(MiruHttpActivitySenderProvider.class);
        bind(MiruPartitionIdProvider.class).to(MiruRCVSPartitionIdProvider.class);

        bind(MiruClient.class).to(clientClass.or(config.getClientClass()));

        install(new MiruClusterModule(config.getHostsStorageClass(), config.getHostsPartitionsStorageClass()));

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

    private List<MiruHost> extractDefaultHosts(MiruClientConfig config) {
        return Lists.transform(Lists.newArrayList(config.getDefaultHostAddresses().split("\\s*,\\s*")), new Function<String, MiruHost>() {
            @Nullable
            @Override
            public MiruHost apply(@Nullable String input) {
                try {
                    String[] hostAddress = input.split("\\s*:\\s*");
                    InetAddress address = InetAddress.getByName(hostAddress[0]);
                    return new MiruHost(address.getHostName(), Integer.parseInt(hostAddress[1]));
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Singleton
    @Provides
    public ObjectMapper provideObjectMapper() {
        return new ObjectMapper();
    }
}