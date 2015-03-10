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
import com.jivesoftware.os.jive.utils.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.cluster.client.MiruClusterClientInitializer;
import com.jivesoftware.os.miru.stumptown.deployable.MiruSoyRendererInitializer.MiruSoyRendererConfig;
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
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;
import com.jivesoftware.os.server.http.jetty.jersey.endpoints.base.HasUI;
import com.jivesoftware.os.server.http.jetty.jersey.server.util.Resource;
import com.jivesoftware.os.upena.main.Deployable;
import com.jivesoftware.os.upena.main.InstanceConfig;
import com.jivesoftware.os.upena.routing.shared.TenantsServiceConnectionDescriptorProvider;
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantRoutingHttpClientInitializer;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.merlin.config.Config;

public class MiruStumptownMain {

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

            deployable.buildStatusReporter(null).start();
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(new HasUI.UI("manage", "manage", "/manage/ui"),
                new HasUI.UI("Stumptown", "main", "/"))));
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(serviceStartupHealthCheck);
            deployable.buildManageServer().start();

            MiruStumptownServiceConfig stumptownServiceConfig = deployable.config(MiruStumptownServiceConfig.class);

            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new GuavaModule());

            RowColumnValueStoreProvider rowColumnValueStoreProvider = stumptownServiceConfig.getRowColumnValueStoreProviderClass()
                .newInstance();
            @SuppressWarnings("unchecked")
            RowColumnValueStoreInitializer<? extends Exception> rowColumnValueStoreInitializer = rowColumnValueStoreProvider
                .create(deployable.config(rowColumnValueStoreProvider.getConfigurationClass()));

            //RowColumnValueStoreInitializer<? extends Exception> rowColumnValueStoreInitializer = new InMemoryRowColumnValueStoreInitializer();
            MiruStumptownPayloads payloads = new MiruStumptownPayloadsIntializer().initialize(instanceConfig.getClusterName(),
                rowColumnValueStoreInitializer, mapper);

            OrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceConfig.getInstanceName()));

            RequestHelper[] miruReaders = RequestHelperUtil.buildRequestHelpers(stumptownServiceConfig.getMiruReaderHosts(), mapper);
            RequestHelper[] miruWrites = RequestHelperUtil.buildRequestHelpers(stumptownServiceConfig.getMiruWriterHosts(), mapper);

            LogMill logMill = new LogMill(orderIdProvider);

            MiruStumptownIntakeConfig intakeConfig = deployable.config(MiruStumptownIntakeConfig.class);

            TenantsServiceConnectionDescriptorProvider connections = deployable.getTenantRoutingProvider().getConnections("miru-manage", // TODO expose to conf
                "main");
            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = new TenantRoutingHttpClientInitializer<>();
            TenantAwareHttpClient<String> miruManageClient = tenantRoutingHttpClientInitializer.initialize(connections);

            // TODO add fall back to config
            //MiruClusterClientConfig clusterClientConfig = deployable.config(MiruClusterClientConfig.class);
            MiruClusterClient clusterClient = new MiruClusterClientInitializer().initialize(miruManageClient, mapper);
            StumptownSchemaService stumptownSchemaService = new StumptownSchemaService(clusterClient);

            MiruStumptownIntakeService inTakeService = new MiruStumptownIntakeInitializer().initialize(intakeConfig,
                stumptownSchemaService,
                logMill,
                miruWrites,
                payloads);

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

            List<MiruManagePlugin> plugins = Lists.newArrayList(
                new MiruManagePlugin("eye-open", "Status", "/stumptown/status",
                    StumptownStatusPluginEndpoints.class,
                    new StumptownStatusPluginRegion("soy.stumptown.page.stumptownStatusPluginRegion", renderer, logMill)),
                new MiruManagePlugin("stats", "Trends", "/stumptown/trends",
                    StumptownTrendsPluginEndpoints.class,
                    new StumptownTrendsPluginRegion("soy.stumptown.page.stumptownTrendsPluginRegion", renderer, miruReaders)),
                new MiruManagePlugin("search", "Query", "/stumptown/query",
                    StumptownQueryPluginEndpoints.class,
                    new StumptownQueryPluginRegion("soy.stumptown.page.stumptownQueryPluginRegion",
                        "soy.stumptown.page.stumptownQueryLogEvent",
                        "soy.stumptown.page.stumptownQueryNoEvents",
                        renderer, miruReaders, payloads)));

            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setContext("/static");

            deployable.addEndpoints(MiruStumptownIntakeEndpoints.class);
            deployable.addInjectables(MiruStumptownIntakeService.class, inTakeService);

            deployable.addEndpoints(MiruQueryStumptownEndpoints.class);
            deployable.addInjectables(MiruStumptownService.class, queryService);

            for (MiruManagePlugin plugin : plugins) {
                queryService.registerPlugin(plugin);
                deployable.addEndpoints(plugin.endpointsClass);
                deployable.addInjectables(plugin.region.getClass(), plugin.region);
            }

            deployable.addResource(sourceTree);
            deployable.buildServer().start();
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
