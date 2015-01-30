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
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.miru.cluster.MiruActivityLookupTable;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryConfig;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.MiruRegistryStoreInitializer;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSActivityLookupTable;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.cluster.rcvs.RegistrySchemaProvider;
import com.jivesoftware.os.miru.cluster.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.logappender.MiruLogAppender;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.manage.deployable.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.manage.deployable.region.AnalyticsPluginRegion;
import com.jivesoftware.os.miru.manage.deployable.region.MiruManagePlugin;
import com.jivesoftware.os.miru.manage.deployable.region.TrendingPluginEndpoints;
import com.jivesoftware.os.miru.manage.deployable.region.TrendingPluginRegion;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReaderImpl;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.MiruWriteToActivityAndSipWAL;
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

        Deployable deployable = new Deployable(args);
        deployable.buildStatusReporter(null).start();
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

        MiruRegistryConfig registryConfig = deployable.config(MiruRegistryConfig.class);

        RowColumnValueStoreProvider rowColumnValueStoreProvider = registryConfig.getRowColumnValueStoreProviderClass()
            .newInstance();
        @SuppressWarnings("unchecked")
        RowColumnValueStoreInitializer<? extends Exception> rowColumnValueStoreInitializer = rowColumnValueStoreProvider
            .create(deployable.config(rowColumnValueStoreProvider.getConfigurationClass()));

        MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);

        MiruRegistryStore registryStore = new MiruRegistryStoreInitializer()
            .initialize(instanceConfig.getClusterName(), rowColumnValueStoreInitializer, mapper);
        MiruClusterRegistry clusterRegistry = new MiruRCVSClusterRegistry(new CurrentTimestamper(),
            registryStore.getHostsRegistry(),
            registryStore.getExpectedTenantsRegistry(),
            registryStore.getExpectedTenantPartitionsRegistry(),
            registryStore.getReplicaRegistry(),
            registryStore.getTopologyRegistry(),
            registryStore.getConfigRegistry(),
            registryStore.getWriterPartitionRegistry(),
            registryConfig.getDefaultNumberOfReplicas(),
            registryConfig.getDefaultTopologyIsStaleAfterMillis());

        MiruWALInitializer.MiruWAL miruWAL = new MiruWALInitializer().initialize(instanceConfig.getClusterName(), rowColumnValueStoreInitializer, mapper);

        MiruActivityWALReader activityWALReader = new MiruActivityWALReaderImpl(miruWAL.getActivityWAL(), miruWAL.getActivitySipWAL());
        MiruActivityWALWriter activityWALWriter = new MiruWriteToActivityAndSipWAL(miruWAL.getActivityWAL(), miruWAL.getActivitySipWAL());
        MiruReadTrackingWALReader readTrackingWALReader = new MiruReadTrackingWALReaderImpl(miruWAL.getReadTrackingWAL(), miruWAL.getReadTrackingSipWAL());
        MiruActivityLookupTable activityLookupTable = new MiruRCVSActivityLookupTable(registryStore.getActivityLookupTable());
        MiruSchemaProvider schemaProvider = new RegistrySchemaProvider(registryStore.getSchemaRegistry(), 10_000);

        MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);

        MiruManageService miruManageService = new MiruManageInitializer().initialize(renderer,
            clusterRegistry,
            schemaProvider,
            activityWALReader,
            readTrackingWALReader,
            activityLookupTable);

        ReaderRequestHelpers readerRequestHelpers = new ReaderRequestHelpers(clusterRegistry, mapper);

        List<MiruManagePlugin> plugins = Lists.newArrayList(
            new MiruManagePlugin("Analytics",
                "/miru/manage/analytics",
                AnalyticsPluginEndpoints.class,
                new AnalyticsPluginRegion("soy.miru.page.analyticsPluginRegion", renderer, readerRequestHelpers)),
            new MiruManagePlugin("Trending",
                "/miru/manage/trending",
                TrendingPluginEndpoints.class,
                new TrendingPluginRegion("soy.miru.page.trendingPluginRegion", renderer, readerRequestHelpers)));

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

        deployable.addResource(sourceTree);
        deployable.buildServer().start();

    }
}
