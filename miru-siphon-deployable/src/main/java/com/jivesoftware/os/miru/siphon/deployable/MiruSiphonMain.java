/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.miru.siphon.deployable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.AmzaInterner;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.client.aquarium.AmzaClientAquariumProvider;
import com.jivesoftware.os.amza.client.collection.AmzaMarshaller;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.client.http.HttpPartitionClientFactory;
import com.jivesoftware.os.amza.client.http.HttpPartitionHostsProvider;
import com.jivesoftware.os.amza.client.http.RingHostHttpClientProvider;
import com.jivesoftware.os.aquarium.AquariumStats;
import com.jivesoftware.os.aquarium.Member;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.cluster.client.MiruClusterClientInitializer;
import com.jivesoftware.os.miru.siphon.api.MiruSiphonLifecycle;
import com.jivesoftware.os.miru.siphon.api.MiruSiphonPlugin;
import com.jivesoftware.os.miru.siphon.deployable.endpoints.MiruSiphonUIPluginEndpoints;
import com.jivesoftware.os.miru.siphon.deployable.region.MiruSiphonPluginRegion;
import com.jivesoftware.os.miru.siphon.deployable.region.MiruSiphonUIPlugin;
import com.jivesoftware.os.miru.siphon.deployable.siphoner.AmzaSiphonerConfig;
import com.jivesoftware.os.miru.siphon.deployable.siphoner.AmzaSiphonerConfigStorage;
import com.jivesoftware.os.miru.siphon.deployable.siphoner.AmzaSiphoners;
import com.jivesoftware.os.miru.siphon.deployable.siphoner.MiruSiphonPluginRegistry;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.deployable.DeployableHealthCheckRegistry;
import com.jivesoftware.os.routing.bird.deployable.ErrorHealthCheckConfig;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.deployable.TenantAwareHttpClientHealthCheck;
import com.jivesoftware.os.routing.bird.endpoints.base.FullyOnlineVersion;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI.UI;
import com.jivesoftware.os.routing.bird.endpoints.base.LoadBalancerHealthCheckEndpoints;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.checkers.FileDescriptorCountHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCPauseHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.LoadAverageHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.routing.bird.health.checkers.SystemCpuHealthChecker;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpDeliveryClientHealthProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.TailAtScaleStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.http.client.TenantRoutingHttpClientInitializer;
import com.jivesoftware.os.routing.bird.server.util.Resource;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptor;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptors;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;
import com.jivesoftware.os.routing.bird.shared.TenantsServiceConnectionDescriptorProvider;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

public class MiruSiphonMain {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static void main(String[] args) throws Exception {
        new MiruSiphonMain().run(args);
    }

    public void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            final Deployable deployable = new Deployable(args);
            HealthFactory.initialize(deployable::config, new DeployableHealthCheckRegistry(deployable));
            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(new UI("Wiki", "main", "/ui/query"))));
            deployable.addHealthCheck(new GCPauseHealthChecker(deployable.config(GCPauseHealthChecker.GCPauseHealthCheckerConfig.class)));
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(new SystemCpuHealthChecker(deployable.config(SystemCpuHealthChecker.SystemCpuHealthCheckerConfig.class)));
            deployable.addHealthCheck(new LoadAverageHealthChecker(deployable.config(LoadAverageHealthChecker.LoadAverageHealthCheckerConfig.class)));
            deployable.addHealthCheck(
                new FileDescriptorCountHealthChecker(deployable.config(FileDescriptorCountHealthChecker.FileDescriptorCountHealthCheckerConfig.class)));
            deployable.addHealthCheck(serviceStartupHealthCheck);
            deployable.addErrorHealthChecks(deployable.config(ErrorHealthCheckConfig.class));
            deployable.addManageInjectables(FullyOnlineVersion.class, (FullyOnlineVersion) () -> {
                if (serviceStartupHealthCheck.startupHasSucceeded()) {
                    return instanceConfig.getVersion();
                } else {
                    return null;
                }
            });
            deployable.buildManageServer().start();

            MiruSiphonConfig siphonConfig = deployable.config(MiruSiphonConfig.class);

            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new GuavaModule());
            mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            HttpDeliveryClientHealthProvider clientHealthProvider = new HttpDeliveryClientHealthProvider(instanceConfig.getInstanceKey(),
                HttpRequestHelperUtils.buildRequestHelper(false, false, null, instanceConfig.getRoutesHost(), instanceConfig.getRoutesPort()),
                instanceConfig.getConnectionsHealth(), 5_000, 100);
            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = deployable.getTenantRoutingHttpClientInitializer();

            TenantRoutingProvider tenantRoutingProvider = deployable.getTenantRoutingProvider();

            TenantsServiceConnectionDescriptorProvider siphonDescriptorProvider = tenantRoutingProvider
                .getConnections(instanceConfig.getServiceName(), "main", 10_000); // TODO config

            TenantAwareHttpClient<String> amzaClient = tenantRoutingHttpClientInitializer.builder(
                deployable.getTenantRoutingProvider().getConnections("amza", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build(); // TODO expose to conf

            deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("amzaClient", amzaClient));

            TenantAwareHttpClient<String> miruWriterClient = tenantRoutingHttpClientInitializer.builder(
                deployable.getTenantRoutingProvider().getConnections("miru-writer", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build(); // TODO expose to conf

            deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("miru-writer", miruWriterClient));

            TenantAwareHttpClient<String> miruManageClient = tenantRoutingHttpClientInitializer.builder(
                deployable.getTenantRoutingProvider().getConnections("miru-manage", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build(); // TODO expose to conf

            deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("miru-manage", miruManageClient));

            MiruSiphonPluginRegistry siphonPluginRegistry = new MiruSiphonPluginRegistry();

            for (String pluginPackage : siphonConfig.getSiphonPackages().split(",")) {
                Reflections reflections = new Reflections(new ConfigurationBuilder()
                    .setUrls(ClasspathHelper.forPackage(pluginPackage.trim()))
                    .setScanners(new SubTypesScanner(), new TypesScanner()));

                Set<Class<? extends MiruSiphonPlugin>> pluginTypes = reflections.getSubTypesOf(MiruSiphonPlugin.class);
                for (Class<? extends MiruSiphonPlugin> pluginType : pluginTypes) {
                    LOG.info("Loading plugin {}", pluginType.getSimpleName());
                    MiruSiphonPlugin plugin = pluginType.newInstance();
                    if (plugin instanceof MiruSiphonLifecycle) {
                        LOG.info("Started lifecycle plugin {}", pluginType.getSimpleName());
                        ((MiruSiphonLifecycle) plugin).start();
                    }
                    siphonPluginRegistry.add(plugin.name(), plugin);
                }
            }

            HttpResponseMapper responseMapper = new HttpResponseMapper(mapper);


            ExecutorService tasExecutors = deployable.newBoundedExecutor(1024, "manage-tas");

            MiruClusterClient clusterClient = new MiruClusterClientInitializer(tasExecutors, 100, 95, 1000).initialize(new MiruStats(), "", miruManageClient,
                mapper);
            MiruSiphonSchemaService miruSiphonSchemaService = new MiruSiphonSchemaService(clusterClient);


            TailAtScaleStrategy tailAtScaleStrategy = new TailAtScaleStrategy(
                deployable.newBoundedExecutor(1024, "tas"),
                100, // TODO config
                95, // TODO config
                1000 // TODO config
            );

            AmzaInterner amzaInterner = new AmzaInterner();
            AmzaClientProvider<HttpClient, HttpClientException> amzaClientProvider = new AmzaClientProvider<>(
                new HttpPartitionClientFactory(),
                new HttpPartitionHostsProvider(amzaClient, tailAtScaleStrategy, mapper),
                new RingHostHttpClientProvider(amzaClient),
                deployable.newBoundedExecutor(siphonConfig.getAmzaCallerThreadPoolSize(), "amza-client"),
                siphonConfig.getAmzaAwaitLeaderElectionForNMillis(),
                -1,
                -1);


            TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(
                new ConstantWriterIdProvider(instanceConfig.getInstanceName()),
                new SnowflakeIdPacker(),
                new JiveEpochTimestampProvider());

            AmzaClientAquariumProvider amzaClientAquariumProvider = new AmzaClientAquariumProvider(new AquariumStats(),
                instanceConfig.getServiceName(),
                amzaClientProvider,
                orderIdProvider,
                new Member(instanceConfig.getInstanceKey().getBytes(StandardCharsets.UTF_8)),
                count -> {
                    ConnectionDescriptors descriptors = siphonDescriptorProvider.getConnections("");
                    int ringSize = descriptors.getConnectionDescriptors().size();
                    return count > ringSize / 2;
                },
                () -> {
                    Set<Member> members = Sets.newHashSet();
                    ConnectionDescriptors descriptors = siphonDescriptorProvider.getConnections("");
                    for (ConnectionDescriptor connectionDescriptor : descriptors.getConnectionDescriptors()) {
                        members.add(new Member(connectionDescriptor.getInstanceDescriptor().instanceKey.getBytes(StandardCharsets.UTF_8)));
                    }
                    return members;
                },
                128, //TODO config
                128, //TODO config
                5_000L, //TODO config
                100L, //TODO config
                60_000L, //TODO config
                10_000L, //TODO config
                Executors.newSingleThreadExecutor(),
                100L, //TODO config
                1_000L, //TODO config
                10_000L,//TODO config
                siphonConfig.getAquariumUseSolutionLog());

            AmzaMarshaller<String> stringMarshaller = new AmzaMarshaller<String>() {
                @Override
                public String fromBytes(byte[] bytes) throws Exception {
                    return new String(bytes, StandardCharsets.UTF_8);
                }

                @Override
                public byte[] toBytes(String value) throws Exception {
                    return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
                }
            };

            AmzaMarshaller<AmzaSiphonerConfig> amzaSiphonerConfigMarshaller = new AmzaMarshaller<AmzaSiphonerConfig>() {
                @Override
                public AmzaSiphonerConfig fromBytes(byte[] bytes) throws Exception {
                    return mapper.readValue(bytes, AmzaSiphonerConfig.class);
                }

                @Override
                public byte[] toBytes(AmzaSiphonerConfig value) throws Exception {
                    return mapper.writeValueAsBytes(value);
                }
            };


            AmzaSiphonerConfigStorage senderConfigStorage = new AmzaSiphonerConfigStorage(
                amzaClientProvider,
                "miru-siphoner-config",
                new PartitionProperties(Durability.fsync_async,
                    0, 0, 0, 0, 0, 0, 0, 0,
                    false,
                    Consistency.leader_quorum,
                    true,
                    true,
                    false,
                    RowType.snappy_primary,
                    "lab",
                    -1,
                    null,
                    -1,
                    -1),
                stringMarshaller,
                amzaSiphonerConfigMarshaller
            );

            MiruSiphonActivityFlusher miruSiphonActivityFlusher = new MiruSiphonActivityFlusher(
                miruSiphonSchemaService,
                siphonConfig.getMiruIngressEndpoint(),
                mapper,
                miruWriterClient
            );

            AmzaSiphoners amzaSiphoners = new AmzaSiphoners(amzaClientProvider,
                amzaClientAquariumProvider,
                deployable.newBoundedExecutor(Runtime.getRuntime().availableProcessors() * 4, "siphoner"),
                "miru-siphon",
                siphonConfig.getSiphonStripingCount(),
                senderConfigStorage,
                siphonPluginRegistry,
                miruSiphonActivityFlusher,
                siphonConfig.getEnsureSiphonersIntervalMillis(),
                mapper);

            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);
            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);
            MiruSiphonUIService miruSiphonUIService = new MiruSiphonServiceInitializer().initialize(renderer);

            MiruSiphonPluginRegion miruSiphonPluginRegion = new MiruSiphonPluginRegion("soy.wikimiru.page.wikiMiruQueryPlugin",
                amzaSiphoners,
                renderer);

            List<MiruSiphonUIPlugin> plugins = Lists.newArrayList();

            plugins.add(new MiruSiphonUIPlugin("search", "Query", "/ui/query",
                MiruSiphonUIPluginEndpoints.class,
                miruSiphonPluginRegion));


            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setDirectoryListingAllowed(false)
                .setContext("/ui/static");

            if (instanceConfig.getMainServiceAuthEnabled()) {
                deployable.addRouteOAuth("/miru/*");
                deployable.addSessionAuth("/ui/*", "/miru/*");
            } else {
                deployable.addNoAuth("/miru/*");
                deployable.addSessionAuth("/ui/*");
            }

            deployable.addInjectables(ObjectMapper.class, mapper);

            deployable.addEndpoints(MiruSiphonEndpoints.class);
            deployable.addInjectables(MiruSiphonUIService.class, miruSiphonUIService);

            for (MiruSiphonUIPlugin plugin : plugins) {
                miruSiphonUIService.registerPlugin(plugin);
                deployable.addEndpoints(plugin.endpointsClass);
                deployable.addInjectables(plugin.region.getClass(), plugin.region);
            }


            deployable.addResource(sourceTree);
            deployable.addEndpoints(LoadBalancerHealthCheckEndpoints.class);
            deployable.buildServer().start();
            clientHealthProvider.start();
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
