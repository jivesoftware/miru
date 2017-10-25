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
import com.jivesoftware.os.miru.cluster.client.MiruClusterClientInitializer;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer.MiruLogAppenderConfig;
import com.jivesoftware.os.miru.logappender.MiruLogEvent;
import com.jivesoftware.os.miru.stumptown.deployable.MiruStumptownIntakeInitializer.MiruStumptownIntakeConfig;
import com.jivesoftware.os.miru.stumptown.deployable.endpoints.StumptownQueryPluginEndpoints;
import com.jivesoftware.os.miru.stumptown.deployable.endpoints.StumptownStatusPluginEndpoints;
import com.jivesoftware.os.miru.stumptown.deployable.endpoints.StumptownTrendsPluginEndpoints;
import com.jivesoftware.os.miru.stumptown.deployable.region.MiruManagePlugin;
import com.jivesoftware.os.miru.stumptown.deployable.region.StumptownQueryPluginRegion;
import com.jivesoftware.os.miru.stumptown.deployable.region.StumptownStatusPluginRegion;
import com.jivesoftware.os.miru.stumptown.deployable.region.StumptownTrendsPluginRegion;
import com.jivesoftware.os.miru.stumptown.deployable.storage.MiruStumptownPayloadStorage;
import com.jivesoftware.os.miru.stumptown.deployable.storage.MiruStumptownPayloadsAmzaIntializer;
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
import com.jivesoftware.os.routing.bird.deployable.config.extractor.ConfigBinder;
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
import com.jivesoftware.os.routing.bird.http.client.HttpDeliveryClientHealthProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.http.client.TenantRoutingHttpClientInitializer;
import com.jivesoftware.os.routing.bird.server.util.Resource;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class MiruStumptownMain {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static void main(String[] args) throws Exception {
        new MiruStumptownMain().run(args);
    }

    void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            ConfigBinder configBinder = new ConfigBinder(args);
            InstanceConfig instanceConfig = configBinder.bind(InstanceConfig.class);
            final Deployable deployable = new Deployable(args, configBinder, instanceConfig, null);

            HealthFactory.initialize(deployable::config, new DeployableHealthCheckRegistry(deployable));
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(new UI("Stumptown", "main", "/ui/query"))));
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

            MiruStumptownConfig miruStumptownConfig = deployable.config(MiruStumptownConfig.class);
            MiruLogAppenderConfig miruLogAppenderConfig = deployable.config(MiruLogAppenderConfig.class);
            MiruStumptownServiceConfig miruStumptownServiceConfig = deployable.config(MiruStumptownServiceConfig.class);

            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new GuavaModule());

            HttpDeliveryClientHealthProvider clientHealthProvider = new HttpDeliveryClientHealthProvider(
                instanceConfig.getInstanceKey(),
                HttpRequestHelperUtils.buildRequestHelper(false, false, null, instanceConfig.getRoutesHost(), instanceConfig.getRoutesPort()),
                instanceConfig.getConnectionsHealth(),
                miruStumptownConfig.getHealthIntervalNMillis(),
                miruStumptownConfig.getHealthSampleWindow());
            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = deployable.getTenantRoutingHttpClientInitializer();

            MiruStumptownPayloadStorage payloads = null;
            try {
                @SuppressWarnings("unchecked")
                TenantAwareHttpClient<String> amzaClient = tenantRoutingHttpClientInitializer.builder(
                    deployable.getTenantRoutingProvider().getConnections("amza", "main", miruStumptownConfig.getRefreshConnectionsAfterNMillis()),
                    clientHealthProvider)
                    .deadAfterNErrors(miruStumptownConfig.getDeadAfterNErrors())
                    .checkDeadEveryNMillis(miruStumptownConfig.getCheckDeadEveryNMillis())
                    .build();

                deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("amza", amzaClient));

                payloads = new MiruStumptownPayloadsAmzaIntializer().initialize(instanceConfig.getClusterName(),
                    amzaClient,
                    miruStumptownConfig.getAwaitLeaderElectionForNMillis(),
                    mapper);
            } catch (Exception x) {
                serviceStartupHealthCheck.info("Failed to setup connection to Amza.", x);
            }

            OrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceConfig.getInstanceName()));

            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> miruWriterClient = tenantRoutingHttpClientInitializer.builder(
                deployable.getTenantRoutingProvider().getConnections("miru-writer", "main", miruStumptownConfig.getRefreshConnectionsAfterNMillis()),
                clientHealthProvider)
                .deadAfterNErrors(miruStumptownConfig.getDeadAfterNErrors())
                .checkDeadEveryNMillis(miruStumptownConfig.getCheckDeadEveryNMillis())
                .build();

            deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("write", miruWriterClient));

            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> miruManageClient = tenantRoutingHttpClientInitializer.builder(
                deployable.getTenantRoutingProvider().getConnections("miru-manage", "main", miruStumptownConfig.getRefreshConnectionsAfterNMillis()),
                clientHealthProvider)
                .deadAfterNErrors(miruStumptownConfig.getDeadAfterNErrors())
                .checkDeadEveryNMillis(miruStumptownConfig.getCheckDeadEveryNMillis())
                .build();

            deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("manage", miruManageClient));

            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> readerClient = tenantRoutingHttpClientInitializer.builder(
                deployable.getTenantRoutingProvider().getConnections("miru-reader", "main", miruStumptownConfig.getRefreshConnectionsAfterNMillis()),
                clientHealthProvider)
                .deadAfterNErrors(miruStumptownConfig.getDeadAfterNErrors())
                .checkDeadEveryNMillis(miruStumptownConfig.getCheckDeadEveryNMillis())
                .build();

            deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("reader", readerClient));

            HttpResponseMapper responseMapper = new HttpResponseMapper(mapper);

            LogMill logMill = new LogMill(orderIdProvider);
            MiruStumptownIntakeConfig intakeConfig = deployable.config(MiruStumptownIntakeConfig.class);

            ExecutorService tasExecutors = deployable.newBoundedExecutor(1024, "manage-tas");

            MiruClusterClient clusterClient = new MiruClusterClientInitializer(tasExecutors, 100, 95, 1000).initialize(new MiruStats(), "", miruManageClient,
                mapper);
            StumptownSchemaService stumptownSchemaService = new StumptownSchemaService(clusterClient);

            final MiruStumptownIntakeService inTakeService = new MiruStumptownIntakeInitializer().initialize(
                miruStumptownServiceConfig.getIngressEnabled(),
                intakeConfig,
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

            new MiruStumptownInternalLogAppender(
                instanceConfig.getDatacenter(),
                instanceConfig.getClusterName(),
                instanceConfig.getHost(),
                instanceConfig.getServiceName(),
                String.valueOf(instanceConfig.getInstanceName()),
                instanceConfig.getVersion(),
                inTakeService,
                10_000,
                1_000,
                miruLogAppenderConfig.getQueueIsBlocking(),
                miruLogAppenderConfig.getIfSuccessPauseMillis(),
                miruLogAppenderConfig.getIfEmptyPauseMillis(),
                miruLogAppenderConfig.getIfErrorPauseMillis(),
                miruLogAppenderConfig.getNonBlockingDrainThreshold(),
                miruLogAppenderConfig.getNonBlockingDrainCount()).install();

            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);
            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);
            MiruStumptownService queryService = new MiruQueryStumptownInitializer().initialize(renderer);

            List<MiruManagePlugin> plugins = Lists.newArrayList(
                new MiruManagePlugin("eye-open", "Status", "/ui/status",
                    StumptownStatusPluginEndpoints.class,
                    new StumptownStatusPluginRegion("soy.stumptown.page.stumptownStatusPluginRegion", renderer, logMill)),
                new MiruManagePlugin("stats", "Trends", "/ui/trends",
                    StumptownTrendsPluginEndpoints.class,
                    new StumptownTrendsPluginRegion("soy.stumptown.page.stumptownTrendsPluginRegion", renderer, readerClient, mapper, responseMapper)),
                new MiruManagePlugin("search", "Query", "/ui/query",
                    StumptownQueryPluginEndpoints.class,
                    new StumptownQueryPluginRegion("soy.stumptown.page.stumptownQueryPluginRegion",
                        "soy.stumptown.page.stumptownQueryLogEvent",
                        "soy.stumptown.page.stumptownQueryNoEvents",
                        renderer, readerClient, mapper, responseMapper, payloads)));

            File staticResourceDir = new File(System.getProperty("user.dir"));
            LOG.info("Static resources rooted at " + staticResourceDir.getAbsolutePath());
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
            deployable.addEndpoints(LoadBalancerHealthCheckEndpoints.class);
            deployable.buildServer().start();
            clientHealthProvider.start();
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
