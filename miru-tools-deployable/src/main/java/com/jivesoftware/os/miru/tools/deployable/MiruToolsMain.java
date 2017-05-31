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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.client.http.HttpPartitionClientFactory;
import com.jivesoftware.os.amza.client.http.HttpPartitionHostsProvider;
import com.jivesoftware.os.amza.client.http.RingHostHttpClientProvider;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.query.filter.FilterStringUtil;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer.MiruLogAppenderConfig;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer.MiruMetricSamplerConfig;
import com.jivesoftware.os.miru.plugin.query.MiruQueryTASRouting;
import com.jivesoftware.os.miru.query.siphon.EdgeWriter;
import com.jivesoftware.os.miru.tools.deployable.endpoints.AggregateCountsPluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.endpoints.AnalyticsPluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.endpoints.CatwalkPluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.endpoints.DistinctsPluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.endpoints.FullTextPluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.endpoints.MiruToolsEndpoints;
import com.jivesoftware.os.miru.tools.deployable.endpoints.RealwavePluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.endpoints.RecoPluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.endpoints.StrutPluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.endpoints.TrendingPluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.endpoints.UniquesPluginEndpoints;
import com.jivesoftware.os.miru.tools.deployable.region.AggregateCountsPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.AnalyticsPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.CatwalkPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.DistinctsPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.FullTextPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.MiruToolsPlugin;
import com.jivesoftware.os.miru.tools.deployable.region.RealwaveFramePluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.RealwavePluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.RecoPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.StrutPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.TrendingPluginRegion;
import com.jivesoftware.os.miru.tools.deployable.region.UniquesPluginRegion;
import com.jivesoftware.os.miru.ui.MiruRegion;
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
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.ArrayUtils;

public class MiruToolsMain {

    public static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static void main(String[] args) throws Exception {
        new MiruToolsMain().run(args);
    }

    void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            final Deployable deployable = new Deployable(args);
            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);

            HealthFactory.initialize(deployable::config, new DeployableHealthCheckRegistry(deployable));
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(new UI("Miru-Tools", "main", "/ui"))));
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


            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            mapper.registerModule(new GuavaModule());

            TenantRoutingProvider tenantRoutingProvider = deployable.getTenantRoutingProvider();
            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = deployable.getTenantRoutingHttpClientInitializer();
            HttpDeliveryClientHealthProvider clientHealthProvider = new HttpDeliveryClientHealthProvider(instanceConfig.getInstanceKey(),
                HttpRequestHelperUtils.buildRequestHelper(false, false, null, instanceConfig.getRoutesHost(), instanceConfig.getRoutesPort()),
                instanceConfig.getConnectionsHealth(), 5_000, 100);

            MiruLogAppenderConfig miruLogAppenderConfig = deployable.config(MiruLogAppenderConfig.class);
            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> miruStumptownClient = tenantRoutingHttpClientInitializer.builder(
                tenantRoutingProvider.getConnections(
                    "miru-stumptown",
                    "main",
                    10_000),
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build();
            new MiruLogAppenderInitializer().initialize(
                instanceConfig.getDatacenter(),
                instanceConfig.getClusterName(),
                instanceConfig.getHost(),
                instanceConfig.getServiceName(),
                String.valueOf(instanceConfig.getInstanceName()),
                instanceConfig.getVersion(),
                miruLogAppenderConfig,
                miruStumptownClient).install();

            MiruMetricSamplerConfig metricSamplerConfig = deployable.config(MiruMetricSamplerConfig.class);
            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> miruAnomalyClient = tenantRoutingHttpClientInitializer.builder(
                tenantRoutingProvider.getConnections(
                    "miru-anomaly",
                    "main",
                    10_000),
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build();
            new MiruMetricSamplerInitializer().initialize(
                instanceConfig.getDatacenter(),
                instanceConfig.getClusterName(),
                instanceConfig.getHost(),
                instanceConfig.getServiceName(),
                String.valueOf(instanceConfig.getInstanceName()),
                instanceConfig.getVersion(),
                metricSamplerConfig,
                miruAnomalyClient).start();

            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);

            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> miruReaderClient = tenantRoutingHttpClientInitializer.builder(
                tenantRoutingProvider.getConnections("miru-reader", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build(); // TODO expose to conf

            deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("reader", miruReaderClient));


            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> amzaClient = tenantRoutingHttpClientInitializer.builder(
                deployable.getTenantRoutingProvider().getConnections("amza", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build(); // TODO expose to conf

            deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("amza", amzaClient));

            HttpResponseMapper responseMapper = new HttpResponseMapper(mapper);

            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);

            MiruToolsService miruToolsService = new MiruToolsInitializer().initialize(
                new MiruStats(),
                instanceConfig.getClusterName(),
                instanceConfig.getInstanceName(),
                renderer,
                tenantRoutingProvider);

            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);


            TailAtScaleStrategy tailAtScaleStrategy = new TailAtScaleStrategy(
                deployable.newBoundedExecutor(1024, "amza-client-tas"),
                100, // TODO config
                95, // TODO config
                1000
            );

            AmzaClientProvider<HttpClient, HttpClientException> amzaClientProvider = new AmzaClientProvider<>(
                new HttpPartitionClientFactory(),
                new HttpPartitionHostsProvider(amzaClient, tailAtScaleStrategy, objectMapper),
                new RingHostHttpClientProvider(amzaClient),
                deployable.newBoundedExecutor(1024, "amza-client"),
                60_000,
                -1,
                -1);

            OrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceConfig.getInstanceName()));
            EdgeWriter edgeWriter = new EdgeWriter(amzaClientProvider,
                orderIdProvider,
                "reader-queries",
                TimeUnit.DAYS.toMillis(30),
                10_000,
                100, mapper);
            edgeWriter.start();

            MiruQueryTASRouting queryTASRouting = new MiruQueryTASRouting(
                miruReaderClient,
                mapper,
                responseMapper,
                deployable.newBoundedExecutor(1024, "reader-tas"),
                100, // TODO config
                95, // TODO config
                1000,
                (tenantId, actorId, family, destination, latency, tags) -> {
                    try {
                        edgeWriter.write(tenantId.toString(), actorId.toString(), family, instanceConfig.getInstanceKey(), destination, latency,
                            (String[])ArrayUtils.addAll(tags, new String[]{
                                "originService:" + instanceConfig.getServiceName(),
                                "originAddress:" + instanceConfig.getHost() + ":" + instanceConfig.getMainPort()
                            }));

                    } catch (Exception x) {
                        LOG.warn("edgewriter failure ", x);
                    }
                });


            FilterStringUtil filterStringUtil = new FilterStringUtil(mapper);
            List<MiruToolsPlugin> plugins = Lists.newArrayList(
                new MiruToolsPlugin("road", "Aggregate Counts",
                    "/ui/tools/aggregate",
                    AggregateCountsPluginEndpoints.class,
                    new AggregateCountsPluginRegion("soy.miru.page.aggregateCountsPluginRegion", renderer, queryTASRouting, filterStringUtil)),
                new MiruToolsPlugin("stats", "Analytics",
                    "/ui/tools/analytics",
                    AnalyticsPluginEndpoints.class,
                    new AnalyticsPluginRegion("soy.miru.page.analyticsPluginRegion", renderer, queryTASRouting, filterStringUtil)),
                new MiruToolsPlugin("education", "Catwalk",
                    "/ui/tools/catwalk",
                    CatwalkPluginEndpoints.class,
                    new CatwalkPluginRegion("soy.miru.page.catwalkPluginRegion", renderer, queryTASRouting, filterStringUtil)),
                new MiruToolsPlugin("fire", "Strut your Stuff",
                    "/ui/tools/strut",
                    StrutPluginEndpoints.class,
                    new StrutPluginRegion("soy.miru.page.strutPluginRegion", renderer, queryTASRouting, filterStringUtil)),
                new MiruToolsPlugin("asterisk", "Distincts",
                    "/ui/tools/distincts",
                    DistinctsPluginEndpoints.class,
                    new DistinctsPluginRegion("soy.miru.page.distinctsPluginRegion", renderer, queryTASRouting, filterStringUtil)),
                new MiruToolsPlugin("zoom-in", "Uniques",
                    "/ui/tools/uniques",
                    UniquesPluginEndpoints.class,
                    new UniquesPluginRegion("soy.miru.page.uniquesPluginRegion", renderer, queryTASRouting, filterStringUtil)),
                new MiruToolsPlugin("search", "Full Text",
                    "/ui/tools/fulltext",
                    FullTextPluginEndpoints.class,
                    new FullTextPluginRegion("soy.miru.page.fullTextPluginRegion", renderer, queryTASRouting, filterStringUtil)),
                new MiruToolsPlugin("flash", "Realwave",
                    "/ui/tools/realwave",
                    RealwavePluginEndpoints.class,
                    new RealwavePluginRegion("soy.miru.page.realwavePluginRegion", renderer, queryTASRouting, filterStringUtil),
                    new RealwaveFramePluginRegion("soy.miru.page.realwaveFramePluginRegion", renderer)),
                new MiruToolsPlugin("thumbs-up", "Reco",
                    "/ui/tools/reco",
                    RecoPluginEndpoints.class,
                    new RecoPluginRegion("soy.miru.page.recoPluginRegion", renderer, queryTASRouting, filterStringUtil)),
                new MiruToolsPlugin("list", "Trending",
                    "/ui/tools/trending",
                    TrendingPluginEndpoints.class,
                    new TrendingPluginRegion("soy.miru.page.trendingPluginRegion", renderer, queryTASRouting, filterStringUtil)));

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
