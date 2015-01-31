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
import com.jivesoftware.os.jive.utils.health.api.HealthCheckConfigBinder;
import com.jivesoftware.os.jive.utils.health.api.HealthCheckRegistry;
import com.jivesoftware.os.jive.utils.health.api.HealthChecker;
import com.jivesoftware.os.jive.utils.health.api.HealthFactory;
import com.jivesoftware.os.jive.utils.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.miru.cluster.MiruRegistryConfig;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.MiruRegistryStoreInitializer;
import com.jivesoftware.os.miru.cluster.rcvs.MiruActivityPayloads;
import com.jivesoftware.os.miru.stumptown.deployable.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.stumptown.deployable.MiruStumptownIntakeInitializer.MiruStumptownIntakeConfig;
import com.jivesoftware.os.miru.stumptown.deployable.endpoints.QueryStumptownPluginEndpoints;
import com.jivesoftware.os.miru.stumptown.deployable.endpoints.StatusStumptownPluginEndpoints;
import com.jivesoftware.os.miru.stumptown.deployable.region.MiruManagePlugin;
import com.jivesoftware.os.miru.stumptown.deployable.region.StumptownQueryPluginRegion;
import com.jivesoftware.os.miru.stumptown.deployable.region.StumptownStatusPluginRegion;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;
import com.jivesoftware.os.server.http.jetty.jersey.server.util.Resource;
import com.jivesoftware.os.upena.main.Deployable;
import com.jivesoftware.os.upena.main.InstanceConfig;
import java.io.File;
import java.util.List;
import org.merlin.config.Config;

public class MiruStumptownMain {

    public static void main(String[] args) throws Exception {
        new MiruStumptownMain().run(args);
    }

    public void run(String[] args) throws Exception {

        final Deployable deployable = new Deployable(args);

        InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);
        //InstanceConfig instanceConfig = deployable.config(DevInstanceConfig.class);

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
        deployable.buildManageServer().start();

        MiruStumptownServiceConfig stumptownServiceConfig = deployable.config(MiruStumptownServiceConfig.class);

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());

        MiruRegistryConfig registryConfig = deployable.config(MiruRegistryConfig.class);

        RowColumnValueStoreProvider rowColumnValueStoreProvider = registryConfig.getRowColumnValueStoreProviderClass()
            .newInstance();
        @SuppressWarnings("unchecked")
        RowColumnValueStoreInitializer<? extends Exception> rowColumnValueStoreInitializer = rowColumnValueStoreProvider
            .create(deployable.config(rowColumnValueStoreProvider.getConfigurationClass()));
        MiruRegistryStore registryStore = new MiruRegistryStoreInitializer().initialize(instanceConfig.getClusterName(),
            rowColumnValueStoreInitializer, mapper);

        OrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceConfig.getInstanceName()));

        RequestHelper[] miruReaders = RequestHelperUtil.buildRequestHelpers(stumptownServiceConfig.getMiruReaderHosts(), mapper);
        RequestHelper[] miruWrites = RequestHelperUtil.buildRequestHelpers(stumptownServiceConfig.getMiruWriterHosts(), mapper);
        MiruActivityPayloads activityPayloads = new MiruActivityPayloads(mapper, registryStore.getActivityPayloadTable());

        LogMill logMill = new LogMill(orderIdProvider);

        MiruStumptownIntakeConfig intakeConfig = deployable.config(MiruStumptownIntakeConfig.class);
        MiruStumptownIntakeService inTakeService = new MiruStumptownIntakeInitializer().initialize(intakeConfig,
            logMill,
            miruWrites,
            miruReaders,
            activityPayloads);

        MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);
        MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);
        MiruQueryStumptownService queryService = new MiruQueryStumptownInitializer().initialize(renderer);

        List<MiruManagePlugin> plugins = Lists.newArrayList(
            new MiruManagePlugin("Query",
                "/stumptown/query",
                QueryStumptownPluginEndpoints.class,
                new StumptownQueryPluginRegion("soy.miru.page.stumptownQueryPluginRegion", renderer, miruReaders, activityPayloads)),
            new MiruManagePlugin("Status",
                "/stumptown/status",
                StatusStumptownPluginEndpoints.class,
                new StumptownStatusPluginRegion("soy.miru.page.stumptownStatusPluginRegion", renderer, logMill)));

        File staticResourceDir = new File(System.getProperty("user.dir"));
        System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
        Resource sourceTree = new Resource(staticResourceDir)
            //.addResourcePath("../../../../../src/main/resources") // fluff?
            .addResourcePath(rendererConfig.getPathToStaticResources())
            .setContext("/static");

        deployable.addEndpoints(MiruStumptownIntakeEndpoints.class);
        deployable.addInjectables(MiruStumptownIntakeService.class, inTakeService);

        deployable.addEndpoints(MiruQueryStumptownEndpoints.class);
        deployable.addInjectables(MiruQueryStumptownService.class, queryService);

        for (MiruManagePlugin plugin : plugins) {
            queryService.registerPlugin(plugin);
            deployable.addEndpoints(plugin.endpointsClass);
            deployable.addInjectables(plugin.region.getClass(), plugin.region);
        }

        deployable.addResource(sourceTree);
        deployable.buildServer().start();

    }
}
