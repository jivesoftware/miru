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
package com.jivesoftware.os.miru.tools.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.logappender.MiruLogAppender;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.logappender.RoutingBirdLogSenderProvider;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSampler;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer.MiruMetricSamplerConfig;
import com.jivesoftware.os.miru.metric.sampler.RoutingBirdMetricSampleSenderProvider;
import com.jivesoftware.os.miru.tools.deployable.endpoints.AggregateCountsPluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.endpoints.AnalyticsPluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.endpoints.CatwalkPluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.endpoints.DistinctsPluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.endpoints.FullTextPluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.endpoints.MiruToolsEndpoints;
import com.jivesoftware.os.miru.tools.deployable.endpoints.RealwavePluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.endpoints.StrutPluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.region.AggregateCountsPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.AnalyticsPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.CatwalkPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.DistinctsPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.FullTextPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.MiruToolsPlugin;
import com.jivesoftware.os.miru.tools.deployable.region.RealwaveFramePluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.RealwavePluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.RecoPluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.region.RecoPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.StrutPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.TrendingPluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.region.TrendingPluginRegion;
import com.jivesoftware.os.miru.ui.MiruRegion;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.deployable.DeployableHealthCheckRegistry;
import com.jivesoftware.os.routing.bird.deployable.ErrorHealthCheckConfig;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI;
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
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;
import com.jivesoftware.os.routing.bird.shared.TenantsServiceConnectionDescriptorProvider;
import java.io.File;
import java.util.Arrays;
import java.util.List;

public class MiruToolsMain {

    public static void main(String[] args) throws Exception {
        new MiruToolsMain().run(args);
    }

    public void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            final Deployable deployable = new Deployable(args);
            HealthFactory.initialize(deployable::config, new DeployableHealthCheckRegistry(deployable));
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(new HasUI.UI("manage", "manage", "/manage/ui"),
                new HasUI.UI("Reset Errors", "manage", "/manage/resetErrors"),
                new HasUI.UI("Reset Health", "manage", "/manage/resetHealth"),
                new HasUI.UI("Tail", "manage", "/manage/tail?lastNLines=1000"),
                new HasUI.UI("Thread Dump", "manage", "/manage/threadDump"),
                new HasUI.UI("Health", "manage", "/manage/ui"),
                new HasUI.UI("Miru-Tools", "main", "/"))));
            deployable.buildStatusReporter(null).start();
            deployable.addHealthCheck(new GCPauseHealthChecker(deployable.config(GCPauseHealthChecker.GCPauseHealthCheckerConfig.class)));
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(new SystemCpuHealthChecker(deployable.config(SystemCpuHealthChecker.SystemCpuHealthCheckerConfig.class)));
            deployable.addHealthCheck(new LoadAverageHealthChecker(deployable.config(LoadAverageHealthChecker.LoadAverageHealthCheckerConfig.class)));
            deployable.addHealthCheck(
                new FileDescriptorCountHealthChecker(deployable.config(FileDescriptorCountHealthChecker.FileDescriptorCountHealthCheckerConfig.class)));
            deployable.addHealthCheck(serviceStartupHealthCheck);
            deployable.addErrorHealthChecks(deployable.config(ErrorHealthCheckConfig.class));
            deployable.buildManageServer().start();

            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class); //config(DevInstanceConfig.class);

            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            mapper.registerModule(new GuavaModule());

            TenantRoutingProvider tenantRoutingProvider = deployable.getTenantRoutingProvider();
            MiruLogAppenderInitializer.MiruLogAppenderConfig miruLogAppenderConfig = deployable.config(MiruLogAppenderInitializer.MiruLogAppenderConfig.class);
            TenantsServiceConnectionDescriptorProvider logConnections = tenantRoutingProvider.getConnections("miru-stumptown", "main", 10_000); // TODO config
            MiruLogAppender miruLogAppender = new MiruLogAppenderInitializer().initialize(null, //TODO datacenter
                instanceConfig.getClusterName(),
                instanceConfig.getHost(),
                instanceConfig.getServiceName(),
                String.valueOf(instanceConfig.getInstanceName()),
                instanceConfig.getVersion(),
                miruLogAppenderConfig,
                new RoutingBirdLogSenderProvider<>(logConnections, "", miruLogAppenderConfig.getSocketTimeoutInMillis()));
            miruLogAppender.install();

            MiruMetricSamplerConfig metricSamplerConfig = deployable.config(MiruMetricSamplerConfig.class);
            TenantsServiceConnectionDescriptorProvider metricConnections = tenantRoutingProvider.getConnections("miru-anomaly", "main", 10_000); // TODO config
            MiruMetricSampler sampler = new MiruMetricSamplerInitializer().initialize(null, //TODO datacenter
                instanceConfig.getClusterName(),
                instanceConfig.getHost(),
                instanceConfig.getServiceName(),
                String.valueOf(instanceConfig.getInstanceName()),
                instanceConfig.getVersion(),
                metricSamplerConfig,
                new RoutingBirdMetricSampleSenderProvider<>(metricConnections, "", miruLogAppenderConfig.getSocketTimeoutInMillis()));
            sampler.start();

            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);

            HttpDeliveryClientHealthProvider clientHealthProvider = new HttpDeliveryClientHealthProvider(instanceConfig.getInstanceKey(),
                HttpRequestHelperUtils.buildRequestHelper(false, false, null, instanceConfig.getRoutesHost(), instanceConfig.getRoutesPort()),
                instanceConfig.getConnectionsHealth(), 5_000, 100);
            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = new TenantRoutingHttpClientInitializer<>();
            TenantAwareHttpClient<String> miruReaderClient = tenantRoutingHttpClientInitializer.builder(
                tenantRoutingProvider.getConnections("miru-reader", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build(); // TODO expose to conf
            HttpResponseMapper responseMapper = new HttpResponseMapper(mapper);

            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);

            MiruToolsService miruToolsService = new MiruToolsInitializer().initialize(instanceConfig.getClusterName(),
                instanceConfig.getInstanceName(),
                renderer,
                tenantRoutingProvider);

            List<MiruToolsPlugin> plugins = Lists.newArrayList(
                new MiruToolsPlugin("road", "Aggregate Counts",
                    "/miru/tools/aggregate",
                    AggregateCountsPluginEndpoints.class,
                    new AggregateCountsPluginRegion("soy.miru.page.aggregateCountsPluginRegion", renderer, miruReaderClient, mapper, responseMapper)),
                new MiruToolsPlugin("stats", "Analytics",
                    "/miru/tools/analytics",
                    AnalyticsPluginEndpoints.class,
                    new AnalyticsPluginRegion("soy.miru.page.analyticsPluginRegion", renderer, miruReaderClient, mapper, responseMapper)),
                new MiruToolsPlugin("education", "Catwalk",
                    "/miru/tools/catwalk",
                    CatwalkPluginEndpoints.class,
                    new CatwalkPluginRegion("soy.miru.page.catwalkPluginRegion", renderer, miruReaderClient, mapper, responseMapper)),
                new MiruToolsPlugin("fire", "Strut your Stuff",
                    "/miru/tools/strut",
                    StrutPluginEndpoints.class,
                    new StrutPluginRegion("soy.miru.page.strutPluginRegion", renderer, miruReaderClient, mapper, responseMapper)),
                new MiruToolsPlugin("asterisk", "Distincts",
                    "/miru/tools/distincts",
                    DistinctsPluginEndpoints.class,
                    new DistinctsPluginRegion("soy.miru.page.distinctsPluginRegion", renderer, miruReaderClient, mapper, responseMapper)),
                new MiruToolsPlugin("search", "Full Text",
                    "/miru/tools/fulltext",
                    FullTextPluginEndpoints.class,
                    new FullTextPluginRegion("soy.miru.page.fullTextPluginRegion", renderer, miruReaderClient, mapper, responseMapper)),
                new MiruToolsPlugin("flash", "Realwave",
                    "/miru/tools/realwave",
                    RealwavePluginEndpoints.class,
                    new RealwavePluginRegion("soy.miru.page.realwavePluginRegion", renderer, miruReaderClient, mapper, responseMapper),
                    new RealwaveFramePluginRegion("soy.miru.page.realwaveFramePluginRegion", renderer)),
                new MiruToolsPlugin("thumbs-up", "Reco",
                    "/miru/tools/reco",
                    RecoPluginEndpoints.class,
                    new RecoPluginRegion("soy.miru.page.recoPluginRegion", renderer, miruReaderClient, mapper, responseMapper)),
                new MiruToolsPlugin("list", "Trending",
                    "/miru/tools/trending",
                    TrendingPluginEndpoints.class,
                    new TrendingPluginRegion("soy.miru.page.trendingPluginRegion", renderer, miruReaderClient, mapper, responseMapper)));

            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setDirectoryListingAllowed(false)
                .setContext("/static");

            deployable.addEndpoints(MiruToolsEndpoints.class);
            deployable.addInjectables(MiruToolsService.class, miruToolsService);

            for (MiruToolsPlugin plugin : plugins) {
                miruToolsService.registerPlugin(plugin);
                deployable.addEndpoints(plugin.endpointsClass);
                deployable.addInjectables(plugin.region.getClass(), plugin.region);
                for (MiruRegion<?> otherRegion : plugin.otherRegions) {
                    deployable.addInjectables(otherRegion.getClass(), otherRegion);
                }
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
