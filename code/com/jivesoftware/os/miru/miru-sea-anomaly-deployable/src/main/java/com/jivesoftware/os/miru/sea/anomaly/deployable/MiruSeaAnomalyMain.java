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
package com.jivesoftware.os.miru.sea.anomaly.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.queue.guaranteed.delivery.DeliveryCallback;
import com.jivesoftware.os.jive.utils.health.api.HealthCheckConfigBinder;
import com.jivesoftware.os.jive.utils.health.api.HealthCheckRegistry;
import com.jivesoftware.os.jive.utils.health.api.HealthChecker;
import com.jivesoftware.os.jive.utils.health.api.HealthFactory;
import com.jivesoftware.os.jive.utils.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.jive.utils.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.ReaderRequestHelpers;
import com.jivesoftware.os.miru.cluster.client.MiruClusterClientInitializer;
import com.jivesoftware.os.miru.logappender.MiruLogAppender;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.metric.sampler.AnomalyMetric;
import com.jivesoftware.os.miru.sea.anomaly.deployable.MiruSeaAnomalyIntakeInitializer.MiruSeaAnomalyIntakeConfig;
import com.jivesoftware.os.miru.sea.anomaly.deployable.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.sea.anomaly.deployable.endpoints.SeaAnomalyQueryPluginEndpoints;
import com.jivesoftware.os.miru.sea.anomaly.deployable.endpoints.SeaAnomalyStatusPluginEndpoints;
import com.jivesoftware.os.miru.sea.anomaly.deployable.endpoints.SeaAnomalyTrendsPluginEndpoints;
import com.jivesoftware.os.miru.sea.anomaly.deployable.region.MiruManagePlugin;
import com.jivesoftware.os.miru.sea.anomaly.deployable.region.SeaAnomalyQueryPluginRegion;
import com.jivesoftware.os.miru.sea.anomaly.deployable.region.SeaAnomalyStatusPluginRegion;
import com.jivesoftware.os.miru.sea.anomaly.deployable.region.SeaAnomalyTrendsPluginRegion;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.server.http.jetty.jersey.endpoints.base.HasUI;
import com.jivesoftware.os.server.http.jetty.jersey.server.util.Resource;
import com.jivesoftware.os.upena.main.Deployable;
import com.jivesoftware.os.upena.main.InstanceConfig;
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantRoutingHttpClientInitializer;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.merlin.config.Config;

public class MiruSeaAnomalyMain {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static void main(String[] args) throws Exception {
        new MiruSeaAnomalyMain().run(args);
    }

    public void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            final Deployable deployable = new Deployable(args);
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
            deployable.buildStatusReporter(null).start();
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(new HasUI.UI("manage", "manage", "/manage/ui"),
                new HasUI.UI("Tail", "manage", "/manage/tail"),
                new HasUI.UI("Thead Dump", "manage", "/manage/threadDump"),
                new HasUI.UI("Health", "manage", "/manage/ui"),
                new HasUI.UI("Anomaly", "main", "/"))));
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(serviceStartupHealthCheck);
            deployable.buildManageServer().start();

            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);

            MiruSeaAnomalyServiceConfig seaAnomalyServiceConfig = deployable.config(MiruSeaAnomalyServiceConfig.class);

            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new GuavaModule());

            OrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceConfig.getInstanceName()));

            serviceStartupHealthCheck.info("building request helpers...", null);

            SampleTrawl sampleTrawl = new SampleTrawl(orderIdProvider);

            MiruSeaAnomalyIntakeConfig intakeConfig = deployable.config(MiruSeaAnomalyIntakeConfig.class);

            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = new TenantRoutingHttpClientInitializer<>();
            TenantAwareHttpClient<String> miruManageClient = tenantRoutingHttpClientInitializer.initialize(deployable
                .getTenantRoutingProvider()
                .getConnections("miru-manage", "main")); // TODO expose to conf

            TenantAwareHttpClient<String> miruWriteClient = tenantRoutingHttpClientInitializer.initialize(deployable
                .getTenantRoutingProvider()
                .getConnections("miru-writer", "main"));  // TODO expose to conf

            // TODO add fall back to config
            //MiruClusterClientConfig clusterClientConfig = deployable.config(MiruClusterClientConfig.class);
            MiruClusterClient clusterClient = new MiruClusterClientInitializer().initialize(new MiruStats(), "", miruManageClient, mapper);
            SeaAnomalySchemaService seaAnomalySchemaService = new SeaAnomalySchemaService(clusterClient);

            final MiruSeaAnomalyIntakeService inTakeService = new MiruSeaAnomalyIntakeInitializer().initialize(intakeConfig,
                seaAnomalySchemaService,
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
                        LOG.error("Encountered the following while draining seaAnomalyQueue.", x);
                        throw new RuntimeException(x);
                    }
                }
            };

            IngressGuaranteedDeliveryQueueProvider ingressGuaranteedDeliveryQueueProvider = new IngressGuaranteedDeliveryQueueProvider(
                intakeConfig.getPathToQueues(), intakeConfig.getNumberOfQueues(), intakeConfig.getNumberOfThreadsPerQueue(), deliveryCallback);

            MiruLogAppenderInitializer.MiruLogAppenderConfig miruLogAppenderConfig = deployable.config(MiruLogAppenderInitializer.MiruLogAppenderConfig.class);
            MiruLogAppender miruLogAppender = new MiruLogAppenderInitializer().initialize(null, //TODO datacenter
                instanceConfig.getClusterName(),
                instanceConfig.getHost(),
                instanceConfig.getServiceName(),
                String.valueOf(instanceConfig.getInstanceName()),
                instanceConfig.getVersion(),
                miruLogAppenderConfig);
            miruLogAppender.install();

            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);
            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);
            MiruSeaAnomalyService queryService = new MiruQuerySeaAnomalyInitializer().initialize(renderer);

            serviceStartupHealthCheck.info("installing ui plugins...", null);

            //TODO expose to config TimeUnit.MINUTES.toMillis(10)
            ReaderRequestHelpers readerRequestHelpers = new ReaderRequestHelpers(clusterClient, mapper, TimeUnit.MINUTES.toMillis(10));
            List<MiruManagePlugin> plugins = Lists.newArrayList(
                new MiruManagePlugin("eye-open", "Status", "/seaAnomaly/status",
                    SeaAnomalyStatusPluginEndpoints.class,
                    new SeaAnomalyStatusPluginRegion("soy.sea.anomaly.page.seaAnomalyStatusPluginRegion", renderer, sampleTrawl)),
                new MiruManagePlugin("stats", "Trends", "/seaAnomaly/trends",
                    SeaAnomalyTrendsPluginEndpoints.class,
                    new SeaAnomalyTrendsPluginRegion("soy.sea.anomaly.page.seaAnomalyTrendsPluginRegion", renderer, readerRequestHelpers)),
                new MiruManagePlugin("search", "Query", "/seaAnomaly/query",
                    SeaAnomalyQueryPluginEndpoints.class,
                    new SeaAnomalyQueryPluginRegion("soy.sea.anomaly.page.seaAnomalyQueryPluginRegion", renderer, readerRequestHelpers)));

            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setContext("/static");

            deployable.addEndpoints(MiruSeaAnomalyIntakeEndpoints.class);
            deployable.addInjectables(IngressGuaranteedDeliveryQueueProvider.class, ingressGuaranteedDeliveryQueueProvider);
            deployable.addInjectables(ObjectMapper.class, mapper);

            deployable.addEndpoints(MiruQuerySeaAnomalyEndpoints.class);
            deployable.addInjectables(MiruSeaAnomalyService.class, queryService);

            for (MiruManagePlugin plugin : plugins) {
                queryService.registerPlugin(plugin);
                deployable.addEndpoints(plugin.endpointsClass);
                deployable.addInjectables(plugin.region.getClass(), plugin.region);
            }

            deployable.addResource(sourceTree);
            serviceStartupHealthCheck.info("start serving...", null);
            deployable.buildServer().start();
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
