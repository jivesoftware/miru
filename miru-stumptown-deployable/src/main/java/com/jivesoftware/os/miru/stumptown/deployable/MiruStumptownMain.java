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
package com.jivesoftware.os.miru.stumptown.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.queue.guaranteed.delivery.DeliveryCallback;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.ReaderRequestHelpers;
import com.jivesoftware.os.miru.cluster.client.MiruClusterClientInitializer;
import com.jivesoftware.os.miru.logappender.MiruLogEvent;
import com.jivesoftware.os.miru.stumptown.deployable.MiruStumptownIntakeInitializer.MiruStumptownIntakeConfig;
import com.jivesoftware.os.miru.stumptown.deployable.endpoints.StumptownQueryPluginEndpoints;
import com.jivesoftware.os.miru.stumptown.deployable.endpoints.StumptownStatusPluginEndpoints;
import com.jivesoftware.os.miru.stumptown.deployable.endpoints.StumptownTrendsPluginEndpoints;
import com.jivesoftware.os.miru.stumptown.deployable.region.MiruManagePlugin;
import com.jivesoftware.os.miru.stumptown.deployable.region.StumptownQueryPluginRegion;
import com.jivesoftware.os.miru.stumptown.deployable.region.StumptownStatusPluginRegion;
import com.jivesoftware.os.miru.stumptown.deployable.region.StumptownTrendsPluginRegion;
import com.jivesoftware.os.miru.stumptown.deployable.storage.MiruStumptownPayloads;
import com.jivesoftware.os.miru.stumptown.deployable.storage.MiruStumptownPayloadsIntializer;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckConfigBinder;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckRegistry;
import com.jivesoftware.os.routing.bird.health.api.HealthChecker;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.routing.bird.http.client.HttpDeliveryClientHealthProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.http.client.TenantRoutingHttpClientInitializer;
import com.jivesoftware.os.routing.bird.server.util.Resource;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.merlin.config.Config;

public class MiruStumptownMain {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static void main(String[] args) throws Exception {
        new MiruStumptownMain().run(args);
    }

    public void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            final Deployable deployable = new Deployable(args);

            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);

            HealthFactory.initialize(new HealthCheckConfigBinder() {

                @Override
                public <C extends Config> C bindConfig(Class<C> configurationInterfaceClass) {
                    return deployable.config(configurationInterfaceClass);
                }
            }, new HealthCheckRegistry() {

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
            deployable.buildStatusReporter(null).start();
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(new HasUI.UI("manage", "manage", "/manage/ui"),
                new HasUI.UI("Reset Errors", "manage", "/manage/resetErrors"),
                new HasUI.UI("Tail", "manage", "/manage/tail"),
                new HasUI.UI("Thead Dump", "manage", "/manage/threadDump"),
                new HasUI.UI("Health", "manage", "/manage/ui"),
                new HasUI.UI("Stumptown", "main", "/"))));
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(serviceStartupHealthCheck);
            deployable.buildManageServer().start();

            MiruStumptownServiceConfig stumptownServiceConfig = deployable.config(MiruStumptownServiceConfig.class);

            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new GuavaModule());


            MiruStumptownPayloads payloads = null;
            try {
                RowColumnValueStoreProvider rowColumnValueStoreProvider = stumptownServiceConfig.getRowColumnValueStoreProviderClass()
                    .newInstance();
                @SuppressWarnings("unchecked")
                RowColumnValueStoreInitializer<? extends Exception> rowColumnValueStoreInitializer = rowColumnValueStoreProvider
                    .create(deployable.config(rowColumnValueStoreProvider.getConfigurationClass()));

                //RowColumnValueStoreInitializer<? extends Exception> rowColumnValueStoreInitializer = new InMemoryRowColumnValueStoreInitializer();
                payloads = new MiruStumptownPayloadsIntializer().initialize(instanceConfig.getClusterName(), rowColumnValueStoreInitializer, mapper);
            } catch (Exception x) {
                serviceStartupHealthCheck.info("Failed to setup connection to RCVS.", x);
            }

            OrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceConfig.getInstanceName()));


            HttpDeliveryClientHealthProvider clientHealthProvider = new HttpDeliveryClientHealthProvider(
                HttpRequestHelperUtils.buildRequestHelper(instanceConfig.getRoutesHost(),instanceConfig.getRoutesPort()),
                instanceConfig.getConnectionsHealth(), 5_000, 100);
            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = new TenantRoutingHttpClientInitializer<>();

            TenantAwareHttpClient<String> miruWriterClient = tenantRoutingHttpClientInitializer.initialize(deployable
                .getTenantRoutingProvider()
                .getConnections("miru-writer", "main"),
                clientHealthProvider,
                10,10_000);  // TODO expose to conf

            TenantAwareHttpClient<String> miruManageClient = tenantRoutingHttpClientInitializer.initialize(deployable
                .getTenantRoutingProvider()
                .getConnections("miru-manage", "main"),
                clientHealthProvider,
                10,10_000); // TODO expose to conf

            LogMill logMill = new LogMill(orderIdProvider);
            MiruStumptownIntakeConfig intakeConfig = deployable.config(MiruStumptownIntakeConfig.class);

            // TODO add fall back to config
            //MiruClusterClientConfig clusterClientConfig = deployable.config(MiruClusterClientConfig.class);
            MiruClusterClient clusterClient = new MiruClusterClientInitializer().initialize(new MiruStats(), "", miruManageClient, mapper);
            StumptownSchemaService stumptownSchemaService = new StumptownSchemaService(clusterClient);

            final MiruStumptownIntakeService inTakeService = new MiruStumptownIntakeInitializer().initialize(intakeConfig,
                stumptownSchemaService,
                logMill,
                mapper,
                miruWriterClient,
                payloads);

            DeliveryCallback deliveryCallback = new JacksonSerializedDeliveryCallback<MiruLogEvent>(intakeConfig.getMaxDrainSize(),
                mapper,
                MiruLogEvent.class,
                true,
                null) {
                @Override
                void deliverSerialized(List<MiruLogEvent> serialized) {
                    try {
                        inTakeService.ingressLogEvents(serialized);
                        LOG.inc("ingress>delivered");
                    } catch (Exception x) {
                        LOG.error("Encountered the following while draining stumptownQueue.", x);
                        throw new RuntimeException(x);
                    }
                }
            };

            IngressGuaranteedDeliveryQueueProvider ingressGuaranteedDeliveryQueueProvider = new IngressGuaranteedDeliveryQueueProvider(
                intakeConfig.getPathToQueues(), intakeConfig.getNumberOfQueues(), intakeConfig.getNumberOfThreadsPerQueue(), deliveryCallback);

            new MiruStumptownInternalLogAppender("unknownDatacenter",
                instanceConfig.getClusterName(),
                instanceConfig.getHost(),
                instanceConfig.getServiceName(),
                String.valueOf(instanceConfig.getInstanceName()),
                instanceConfig.getVersion(),
                inTakeService,
                10_000,
                1_000,
                false,
                1_000,
                1_000,
                5_000,
                1_000,
                1_000,
                10_000).install();

            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);
            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);
            MiruStumptownService queryService = new MiruQueryStumptownInitializer().initialize(renderer);

            //TODO expose to config TimeUnit.MINUTES.toMillis(10)
            ReaderRequestHelpers readerRequestHelpers = new ReaderRequestHelpers(clusterClient, mapper, TimeUnit.MINUTES.toMillis(10));
            List<MiruManagePlugin> plugins = Lists.newArrayList(
                new MiruManagePlugin("eye-open", "Status", "/stumptown/status",
                    StumptownStatusPluginEndpoints.class,
                    new StumptownStatusPluginRegion("soy.stumptown.page.stumptownStatusPluginRegion", renderer, logMill)),
                new MiruManagePlugin("stats", "Trends", "/stumptown/trends",
                    StumptownTrendsPluginEndpoints.class,
                    new StumptownTrendsPluginRegion("soy.stumptown.page.stumptownTrendsPluginRegion", renderer, readerRequestHelpers)),
                new MiruManagePlugin("search", "Query", "/stumptown/query",
                    StumptownQueryPluginEndpoints.class,
                    new StumptownQueryPluginRegion("soy.stumptown.page.stumptownQueryPluginRegion",
                        "soy.stumptown.page.stumptownQueryLogEvent",
                        "soy.stumptown.page.stumptownQueryNoEvents",
                        renderer, readerRequestHelpers, payloads)));

            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setContext("/static");

            deployable.addEndpoints(MiruStumptownIntakeEndpoints.class);
            deployable.addInjectables(IngressGuaranteedDeliveryQueueProvider.class, ingressGuaranteedDeliveryQueueProvider);
            deployable.addInjectables(ObjectMapper.class, mapper);

            deployable.addEndpoints(MiruQueryStumptownEndpoints.class);
            deployable.addInjectables(MiruStumptownService.class, queryService);

            for (MiruManagePlugin plugin : plugins) {
                queryService.registerPlugin(plugin);
                deployable.addEndpoints(plugin.endpointsClass);
                deployable.addInjectables(plugin.region.getClass(), plugin.region);
            }

            deployable.addResource(sourceTree);
            deployable.buildServer().start();
            clientHealthProvider.start();
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
