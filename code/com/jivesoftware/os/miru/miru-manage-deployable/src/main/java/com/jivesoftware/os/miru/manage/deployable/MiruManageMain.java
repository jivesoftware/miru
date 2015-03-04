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
import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.jive.utils.health.api.HealthCheckConfigBinder;
import com.jivesoftware.os.jive.utils.health.api.HealthCheckRegistry;
import com.jivesoftware.os.jive.utils.health.api.HealthChecker;
import com.jivesoftware.os.jive.utils.health.api.HealthFactory;
import com.jivesoftware.os.jive.utils.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.jive.utils.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.marshall.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.api.topology.MiruRegistryConfig;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryClusterClient;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.MiruRegistryStoreInitializer;
import com.jivesoftware.os.miru.cluster.amza.AmzaClusterRegistry;
import com.jivesoftware.os.miru.cluster.amza.AmzaClusterRegistryInitializer;
import com.jivesoftware.os.miru.cluster.amza.AmzaClusterRegistryInitializer.AmzaClusterRegistryConfig;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.logappender.MiruLogAppender;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.manage.deployable.region.AggregateCountsPluginRegion;
import com.jivesoftware.os.miru.manage.deployable.region.AnalyticsPluginRegion;
import com.jivesoftware.os.miru.manage.deployable.region.DistinctsPluginRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruManagePlugin;
import com.jivesoftware.os.miru.manage.deployable.region.RecoPluginEndpoints;
import com.jivesoftware.os.miru.manage.deployable.region.RecoPluginRegion;
import com.jivesoftware.os.miru.manage.deployable.region.TrendingPluginEndpoints;
import com.jivesoftware.os.miru.manage.deployable.region.TrendingPluginRegion;
import com.jivesoftware.os.miru.manage.deployable.topology.MiruTopologyEndpoints;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSampler;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer.MiruMetricSamplerConfig;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReaderImpl;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.MiruWriteToActivityAndSipWAL;
import com.jivesoftware.os.miru.wal.lookup.MiruActivityLookupTable;
import com.jivesoftware.os.miru.wal.lookup.MiruRCVSActivityLookupTable;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;
import com.jivesoftware.os.rcvs.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.server.http.jetty.jersey.server.util.Resource;
import com.jivesoftware.os.upena.main.Deployable;
import com.jivesoftware.os.upena.main.InstanceConfig;
import java.io.File;
import java.util.List;
import org.merlin.config.Config;

public class MiruManageMain {

    public static void main(String[] args) throws Exception {
        new MiruManageMain().run(args);
    }

    /*
     private interface DevInstanceConfig extends InstanceConfig {

     @StringDefault("dev")
     String getClusterName();
     }
     */
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

            RowColumnValueStoreProvider rowColumnValueStoreProvider = registryConfig.getRowColumnValueStoreProviderClass()
                .newInstance();
            @SuppressWarnings("unchecked")
            RowColumnValueStoreInitializer<? extends Exception> rowColumnValueStoreInitializer = rowColumnValueStoreProvider
                .create(deployable.config(rowColumnValueStoreProvider.getConfigurationClass()));

            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);

            MiruRegistryStore registryStore = new MiruRegistryStoreInitializer()
                .initialize(instanceConfig.getClusterName(), rowColumnValueStoreInitializer, mapper);

            MiruClusterRegistry clusterRegistry;
            if (registryConfig.getClusterRegistryType().equals("hbase")) {
                clusterRegistry = new MiruRCVSClusterRegistry(new CurrentTimestamper(),
                    registryStore.getHostsRegistry(),
                    registryStore.getExpectedTenantsRegistry(),
                    registryStore.getTopologyUpdatesRegistry(),
                    registryStore.getExpectedTenantPartitionsRegistry(),
                    registryStore.getReplicaRegistry(),
                    registryStore.getTopologyRegistry(),
                    registryStore.getConfigRegistry(),
                    registryStore.getSchemaRegistry(),
                    registryConfig.getDefaultNumberOfReplicas(),
                    registryConfig.getDefaultTopologyIsStaleAfterMillis(),
                    registryConfig.getDefaultTopologyIsIdleAfterMillis());
            } else {
                AmzaClusterRegistryConfig amzaClusterRegistryConfig = deployable.config(AmzaClusterRegistryConfig.class);
                AmzaService amzaService = new AmzaClusterRegistryInitializer().initialize(deployable,
                    instanceConfig.getInstanceName(),
                    instanceConfig.getHost(),
                    instanceConfig.getMainPort(),
                    "amza-topology-" + instanceConfig.getClusterName(),
                    amzaClusterRegistryConfig);
                clusterRegistry = new AmzaClusterRegistry(amzaService,
                    new JacksonJsonObjectTypeMarshaller<>(MiruSchema.class, mapper),
                    registryConfig.getDefaultNumberOfReplicas(),
                    registryConfig.getDefaultTopologyIsStaleAfterMillis(),
                    registryConfig.getDefaultTopologyIsIdleAfterMillis());
            }

            MiruWALInitializer.MiruWAL miruWAL = new MiruWALInitializer().initialize(instanceConfig.getClusterName(), rowColumnValueStoreInitializer, mapper);

            MiruActivityWALReader activityWALReader = new MiruActivityWALReaderImpl(miruWAL.getActivityWAL(),
                miruWAL.getActivitySipWAL(),
                miruWAL.getWriterPartitionRegistry());

            MiruActivityWALWriter activityWALWriter = new MiruWriteToActivityAndSipWAL(miruWAL.getActivityWAL(), miruWAL.getActivitySipWAL());
            MiruReadTrackingWALReader readTrackingWALReader = new MiruReadTrackingWALReaderImpl(miruWAL.getReadTrackingWAL(), miruWAL.getReadTrackingSipWAL());
            MiruActivityLookupTable activityLookupTable = new MiruRCVSActivityLookupTable(miruWAL.getActivityLookupTable());

            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);

            MiruManageService miruManageService = new MiruManageInitializer().initialize(renderer,
                clusterRegistry,
                activityWALReader,
                readTrackingWALReader,
                activityLookupTable);

            ReaderRequestHelpers readerRequestHelpers = new ReaderRequestHelpers(clusterRegistry, mapper);

            List<MiruManagePlugin> plugins = Lists.newArrayList(
                new MiruManagePlugin("Distincts",
                    "/miru/manage/distincts",
                    DistinctsPluginEndpoints.class,
                    new DistinctsPluginRegion("soy.miru.page.distinctsPluginRegion", renderer, readerRequestHelpers)),
                new MiruManagePlugin("Analytics",
                    "/miru/manage/analytics",
                    AnalyticsPluginEndpoints.class,
                    new AnalyticsPluginRegion("soy.miru.page.analyticsPluginRegion", renderer, readerRequestHelpers)),
                new MiruManagePlugin("Trending",
                    "/miru/manage/trending",
                    TrendingPluginEndpoints.class,
                    new TrendingPluginRegion("soy.miru.page.trendingPluginRegion", renderer, readerRequestHelpers)),
                new MiruManagePlugin("Reco",
                    "/miru/manage/reco",
                    RecoPluginEndpoints.class,
                    new RecoPluginRegion("soy.miru.page.recoPluginRegion", renderer, readerRequestHelpers)),
                new MiruManagePlugin("Aggregate Counts",
                    "/miru/manage/aggregate",
                    AggregateCountsPluginEndpoints.class,
                    new AggregateCountsPluginRegion("soy.miru.page.aggregateCountsPluginRegion", renderer, readerRequestHelpers)));

            MiruRebalanceDirector rebalanceDirector = new MiruRebalanceInitializer().initialize(clusterRegistry,
                new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceConfig.getInstanceName())), readerRequestHelpers);

            MiruWALDirector walDirector = new MiruWALDirector(clusterRegistry, activityWALReader, activityWALWriter);

            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                //.addResourcePath("../../../../../src/main/resources") // fluff?
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setContext("/static");

            deployable.addEndpoints(MiruManageEndpoints.class);
            deployable.addInjectables(MiruManageService.class, miruManageService);
            deployable.addInjectables(MiruRebalanceDirector.class, rebalanceDirector);
            deployable.addInjectables(MiruWALDirector.class, walDirector);

            for (MiruManagePlugin plugin : plugins) {
                miruManageService.registerPlugin(plugin);
                deployable.addEndpoints(plugin.endpointsClass);
                deployable.addInjectables(plugin.region.getClass(), plugin.region);
            }

            deployable.addEndpoints(MiruTopologyEndpoints.class);
            deployable.addInjectables(MiruRegistryClusterClient.class, new MiruRegistryClusterClient(clusterRegistry));

            deployable.addResource(sourceTree);
            deployable.buildServer().start();
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
