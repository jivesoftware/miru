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
package com.jivesoftware.os.miru.manage.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.jivesoftware.os.amza.client.AmzaKretrProvider;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.storage.PartitionProvider;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceConfig;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceInitializer;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.marshall.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruRegistryConfig;
import com.jivesoftware.os.miru.api.topology.ReaderRequestHelpers;
import com.jivesoftware.os.miru.api.wal.AmzaCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALConfig;
import com.jivesoftware.os.miru.api.wal.RCVSCursor;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.cluster.MiruRegistryClusterClient;
import com.jivesoftware.os.miru.cluster.MiruReplicaSetDirector;
import com.jivesoftware.os.miru.cluster.amza.AmzaClusterRegistry;
import com.jivesoftware.os.miru.logappender.MiruLogAppender;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.manage.deployable.balancer.MiruRebalanceDirector;
import com.jivesoftware.os.miru.manage.deployable.balancer.MiruRebalanceInitializer;
import com.jivesoftware.os.miru.manage.deployable.topology.MiruTopologyEndpoints;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSampler;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer.MiruMetricSamplerConfig;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.wal.client.MiruWALClientInitializer;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckRegistry;
import com.jivesoftware.os.routing.bird.health.api.HealthChecker;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.http.client.TenantRoutingHttpClientInitializer;
import com.jivesoftware.os.routing.bird.server.util.Resource;
import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public class MiruManageMain {

    public static void main(String[] args) throws Exception {
        new MiruManageMain().run(args);
    }

    public interface AmzaClusterRegistryConfig extends MiruAmzaServiceConfig {

        @StringDefault("./var/amza/registry/data/")
        @Override
        String getWorkingDirectories();

        @StringDefault("./var/amza/registry/index/")
        @Override
        String getIndexDirectories();

        @StringDefault("225.5.6.25")
        @Override
        String getAmzaDiscoveryGroup();

        @IntDefault(1225)
        @Override
        int getAmzaDiscoveryPort();

        @IntDefault(1)
        int getReplicateTakeQuorum();

        @LongDefault(60_000L)
        long getReplicateTimeoutMillis();
    }

    public void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            final Deployable deployable = new Deployable(args);
            HealthFactory.initialize(deployable::config,
                new HealthCheckRegistry() {

                    @Override
                    public void register(HealthChecker healthChecker) {
                        deployable.addHealthCheck(healthChecker);
                    }

                    @Override
                    public void unregister(HealthChecker healthChecker) {
                        throw new UnsupportedOperationException("Not supported yet.");
                    }
                });
            deployable.addErrorHealthChecks();
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(
                new HasUI.UI("Reset Errors", "manage", "/manage/resetErrors"),
                new HasUI.UI("Tail", "manage", "/manage/tail"),
                new HasUI.UI("Thead Dump", "manage", "/manage/threadDump"),
                new HasUI.UI("Health", "manage", "/manage/ui"),
                new HasUI.UI("Miru-Manage", "main", "/miru/manage"),
                new HasUI.UI("Miru-Manage-Amza", "main", "/amza"))));
            deployable.buildStatusReporter(null).start();
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(serviceStartupHealthCheck);
            deployable.buildManageServer().start();

            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class); //config(DevInstanceConfig.class);

            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            mapper.registerModule(new GuavaModule());

            MiruLogAppenderInitializer.MiruLogAppenderConfig miruLogAppenderConfig = deployable.config(MiruLogAppenderInitializer.MiruLogAppenderConfig.class);
            MiruLogAppender miruLogAppender = new MiruLogAppenderInitializer().initialize(null, //TODO datacenter
                instanceConfig.getClusterName(),
                instanceConfig.getHost(),
                instanceConfig.getServiceName(),
                String.valueOf(instanceConfig.getInstanceName()),
                instanceConfig.getVersion(),
                miruLogAppenderConfig);
            miruLogAppender.install();

            MiruMetricSamplerConfig metricSamplerConfig = deployable.config(MiruMetricSamplerConfig.class);
            MiruMetricSampler sampler = new MiruMetricSamplerInitializer().initialize(null, //TODO datacenter
                instanceConfig.getClusterName(),
                instanceConfig.getHost(),
                instanceConfig.getServiceName(),
                String.valueOf(instanceConfig.getInstanceName()),
                instanceConfig.getVersion(),
                metricSamplerConfig);
            sampler.start();

            MiruRegistryConfig registryConfig = deployable.config(MiruRegistryConfig.class);
            MiruWALConfig walConfig = deployable.config(MiruWALConfig.class);
            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);

            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = new TenantRoutingHttpClientInitializer<>();
            TenantAwareHttpClient<String> walHttpClient = tenantRoutingHttpClientInitializer.initialize(deployable
                .getTenantRoutingProvider()
                .getConnections("miru-wal", "main")); // TODO expose to conf

            MiruWALClient<?, ?> miruWALClient;
            if (walConfig.getActivityWALType().equals("rcvs") || walConfig.getActivityWALType().equals("rcvs_amza")) {
                MiruWALClient<RCVSCursor, RCVSSipCursor> rcvsWALClient = new MiruWALClientInitializer().initialize("", walHttpClient, mapper, 10_000,
                    "/miru/wal/rcvs", RCVSCursor.class, RCVSSipCursor.class);
                miruWALClient = rcvsWALClient;
            } else if (walConfig.getActivityWALType().equals("amza") || walConfig.getActivityWALType().equals("amza_rcvs")) {
                MiruWALClient<AmzaCursor, AmzaSipCursor> amzaWALClient = new MiruWALClientInitializer().initialize("", walHttpClient, mapper, 10_000,
                    "/miru/wal/amza", AmzaCursor.class, AmzaSipCursor.class);
                miruWALClient = amzaWALClient;
            } else {
                throw new IllegalStateException("Invalid activity WAL type: " + walConfig.getActivityWALType());
            }

            AmzaClusterRegistryConfig amzaClusterRegistryConfig = deployable.config(AmzaClusterRegistryConfig.class);
            AmzaService amzaService = new MiruAmzaServiceInitializer().initialize(deployable,
                instanceConfig.getInstanceName(),
                instanceConfig.getInstanceKey(),
                instanceConfig.getHost(),
                instanceConfig.getMainPort(),
                "amza-topology-" + instanceConfig.getClusterName(),
                amzaClusterRegistryConfig,
                rowsChanged -> {
                });
            AmzaKretrProvider amzaKretrProvider = new AmzaKretrProvider(amzaService);
            AmzaClusterRegistry clusterRegistry = new AmzaClusterRegistry(amzaService,
                amzaKretrProvider,
                amzaClusterRegistryConfig.getReplicateTakeQuorum(),
                amzaClusterRegistryConfig.getReplicateTimeoutMillis(),
                new JacksonJsonObjectTypeMarshaller<>(MiruSchema.class, mapper),
                registryConfig.getDefaultNumberOfReplicas(),
                registryConfig.getDefaultTopologyIsStaleAfterMillis(),
                registryConfig.getDefaultTopologyIsIdleAfterMillis(),
                registryConfig.getDefaultTopologyDestroyAfterMillis(),
                amzaClusterRegistryConfig.getTakeFromFactor());
            amzaService.watch(PartitionProvider.RING_INDEX.getPartitionName(), clusterRegistry);

            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);

            MiruStats stats = new MiruStats();

            MiruManageService miruManageService = new MiruManageInitializer().initialize(renderer,
                clusterRegistry,
                miruWALClient,
                stats);

            OrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(0), new SnowflakeIdPacker(),
                new JiveEpochTimestampProvider());
            MiruClusterClient clusterClient = new MiruRegistryClusterClient(clusterRegistry, new MiruReplicaSetDirector(orderIdProvider, clusterRegistry));
            //TODO expose to config TimeUnit.MINUTES.toMillis(10)
            ReaderRequestHelpers readerRequestHelpers = new ReaderRequestHelpers(clusterClient, mapper, TimeUnit.MINUTES.toMillis(10));

            MiruRebalanceDirector rebalanceDirector = new MiruRebalanceInitializer().initialize(clusterRegistry, miruWALClient,
                new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceConfig.getInstanceName())), readerRequestHelpers);

            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                //.addResourcePath("../../../../../src/main/resources") // fluff?
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setContext("/static");

            deployable.addEndpoints(MiruManageEndpoints.class);
            deployable.addInjectables(MiruManageService.class, miruManageService);
            deployable.addInjectables(MiruRebalanceDirector.class, rebalanceDirector);
            deployable.addInjectables(MiruWALClient.class, miruWALClient);
            deployable.addEndpoints(MiruTopologyEndpoints.class);
            deployable.addInjectables(MiruStats.class, stats);
            deployable.addInjectables(MiruRegistryClusterClient.class, clusterClient);

            deployable.addResource(sourceTree);
            deployable.buildServer().start();
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
