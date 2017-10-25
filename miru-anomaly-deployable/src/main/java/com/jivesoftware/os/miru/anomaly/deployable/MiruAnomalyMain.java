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
package com.jivesoftware.os.miru.anomaly.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.queue.guaranteed.delivery.DeliveryCallback;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.miru.anomaly.deployable.MiruAnomalyIntakeInitializer.MiruAnomalyIntakeConfig;
import com.jivesoftware.os.miru.anomaly.deployable.endpoints.AnomalyQueryPluginEndpoints;
import com.jivesoftware.os.miru.anomaly.deployable.endpoints.AnomalyStatusPluginEndpoints;
import com.jivesoftware.os.miru.anomaly.deployable.region.AnomalyPlugin;
import com.jivesoftware.os.miru.anomaly.deployable.region.AnomalyQueryPluginRegion;
import com.jivesoftware.os.miru.anomaly.deployable.region.AnomalyStatusPluginRegion;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.cluster.client.MiruClusterClientInitializer;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer.MiruLogAppenderConfig;
import com.jivesoftware.os.miru.metric.sampler.AnomalyMetric;
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

public class MiruAnomalyMain {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static void main(String[] args) throws Exception {
        new MiruAnomalyMain().run(args);
    }

    void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            ConfigBinder configBinder = new ConfigBinder(args);
            InstanceConfig instanceConfig = configBinder.bind(InstanceConfig.class);
            final Deployable deployable = new Deployable(args, configBinder, instanceConfig, null);

            HealthFactory.initialize(deployable::config, new DeployableHealthCheckRegistry(deployable));
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(new UI("Anomaly", "main", "/ui/anomaly/query"))));
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

            MiruAnomalyServiceConfig anomalyServiceConfig = deployable.config(MiruAnomalyServiceConfig.class);

            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new GuavaModule());

            OrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceConfig.getInstanceName()));

            serviceStartupHealthCheck.info("building request helpers...", null);

            SampleTrawl sampleTrawl = new SampleTrawl(orderIdProvider);

            MiruAnomalyIntakeConfig intakeConfig = deployable.config(MiruAnomalyIntakeConfig.class);

            HttpDeliveryClientHealthProvider clientHealthProvider = new HttpDeliveryClientHealthProvider(instanceConfig.getInstanceKey(),
                HttpRequestHelperUtils.buildRequestHelper(false, false, null, instanceConfig.getRoutesHost(), instanceConfig.getRoutesPort()),
                instanceConfig.getConnectionsHealth(), 5_000, 100);

            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = deployable.getTenantRoutingHttpClientInitializer();
            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> miruManageClient = tenantRoutingHttpClientInitializer.builder(deployable
                    .getTenantRoutingProvider()
                    .getConnections("miru-manage", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build(); // TODO expose to conf

            deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("manage", miruManageClient));

            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> miruWriteClient = tenantRoutingHttpClientInitializer.builder(deployable
                    .getTenantRoutingProvider()
                    .getConnections("miru-writer", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build(); // TODO expose to conf

            deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("writer", miruWriteClient));

            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> readerClient = tenantRoutingHttpClientInitializer.builder(deployable
                    .getTenantRoutingProvider()
                    .getConnections("miru-reader", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build(); // TODO expose to conf

            deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("reader", readerClient));

            HttpResponseMapper responseMapper = new HttpResponseMapper(mapper);

            ExecutorService tasExecutors = deployable.newBoundedExecutor(1024, "manage-tas");

            MiruClusterClient clusterClient = new MiruClusterClientInitializer(tasExecutors, 100, 95, 1000).initialize(new MiruStats(), "", miruManageClient, mapper);
            AnomalySchemaService anomalySchemaService = new AnomalySchemaService(clusterClient);

            final MiruAnomalyIntakeService inTakeService = new MiruAnomalyIntakeInitializer().initialize(anomalyServiceConfig.getIngressEnabled(),
                intakeConfig,
                anomalySchemaService,
                sampleTrawl,
                mapper,
                miruWriteClient);

            DeliveryCallback deliveryCallback = new JacksonSerializedDeliveryCallback<AnomalyMetric>(intakeConfig.getMaxDrainSize(),
                mapper,
                AnomalyMetric.class,
                true,
                null) {
                @Override
                void deliverSerialized(List<AnomalyMetric> serialized) {
                    try {
                        inTakeService.ingressEvents(serialized);
                        LOG.inc("ingress>delivered");
                    } catch (Exception x) {
                        LOG.error("Encountered the following while draining anomalyQueue.", x);
                        throw new RuntimeException(x);
                    }
                }
            };

            IngressGuaranteedDeliveryQueueProvider ingressGuaranteedDeliveryQueueProvider = new IngressGuaranteedDeliveryQueueProvider(
                intakeConfig.getPathToQueues(), intakeConfig.getNumberOfQueues(), intakeConfig.getNumberOfThreadsPerQueue(), deliveryCallback);

            MiruLogAppenderConfig miruLogAppenderConfig = deployable.config(MiruLogAppenderConfig.class);
            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> miruStumptownClient = tenantRoutingHttpClientInitializer.builder(
                deployable.getTenantRoutingProvider().getConnections(
                    "miru-stumptown",
                    "main",
                    10_000),
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build();

            deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("stump", miruStumptownClient));

            new MiruLogAppenderInitializer().initialize(
                instanceConfig.getDatacenter(),
                instanceConfig.getClusterName(),
                instanceConfig.getHost(),
                instanceConfig.getServiceName(),
                String.valueOf(instanceConfig.getInstanceName()),
                instanceConfig.getVersion(),
                miruLogAppenderConfig,
                miruStumptownClient).install();

            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);
            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);
            MiruAnomalyService queryService = new MiruQueryAnomalyInitializer().initialize(renderer);

            serviceStartupHealthCheck.info("installing ui plugins...", null);

            List<AnomalyPlugin> plugins = Lists.newArrayList(new AnomalyPlugin("eye-open", "Status", "/ui/anomaly/status",
                    AnomalyStatusPluginEndpoints.class,
                    new AnomalyStatusPluginRegion("soy.anomaly.page.anomalyStatusPluginRegion", renderer, sampleTrawl)),
                new AnomalyPlugin("search", "Query", "/ui/anomaly/query",
                    AnomalyQueryPluginEndpoints.class,
                    new AnomalyQueryPluginRegion("soy.anomaly.page.anomalyQueryPluginRegion", renderer, readerClient, mapper, responseMapper)));

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

            deployable.addEndpoints(MiruAnomalyIntakeEndpoints.class);
            deployable.addInjectables(IngressGuaranteedDeliveryQueueProvider.class, ingressGuaranteedDeliveryQueueProvider);
            deployable.addInjectables(ObjectMapper.class, mapper);

            deployable.addEndpoints(MiruQueryAnomalyEndpoints.class);
            deployable.addInjectables(MiruAnomalyService.class, queryService);

            for (AnomalyPlugin plugin : plugins) {
                queryService.registerPlugin(plugin);
                deployable.addEndpoints(plugin.endpointsClass);
                deployable.addInjectables(plugin.region.getClass(), plugin.region);
            }

            deployable.addResource(sourceTree);
            serviceStartupHealthCheck.info("start serving...", null);
            deployable.addEndpoints(LoadBalancerHealthCheckEndpoints.class);
            deployable.buildServer().start();
            clientHealthProvider.start();
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }

}
