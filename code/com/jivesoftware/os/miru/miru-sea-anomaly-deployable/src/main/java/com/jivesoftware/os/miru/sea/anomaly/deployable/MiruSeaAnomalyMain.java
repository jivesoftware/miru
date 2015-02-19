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
import com.jivesoftware.os.miru.logappender.MiruLogAppender;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.sea.anomaly.deployable.MiruSeaAnomalyIntakeInitializer.MiruSeaAnomalyIntakeConfig;
import com.jivesoftware.os.miru.sea.anomaly.deployable.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.sea.anomaly.deployable.endpoints.SeaAnomalyQueryPluginEndpoints;
import com.jivesoftware.os.miru.sea.anomaly.deployable.endpoints.SeaAnomalyStatusPluginEndpoints;
import com.jivesoftware.os.miru.sea.anomaly.deployable.endpoints.SeaAnomalyTrendsPluginEndpoints;
import com.jivesoftware.os.miru.sea.anomaly.deployable.region.MiruManagePlugin;
import com.jivesoftware.os.miru.sea.anomaly.deployable.region.SeaAnomalyQueryPluginRegion;
import com.jivesoftware.os.miru.sea.anomaly.deployable.region.SeaAnomalyStatusPluginRegion;
import com.jivesoftware.os.miru.sea.anomaly.deployable.region.SeaAnomalyTrendsPluginRegion;
import com.jivesoftware.os.server.http.jetty.jersey.server.util.Resource;
import com.jivesoftware.os.upena.main.Deployable;
import com.jivesoftware.os.upena.main.InstanceConfig;
import java.io.File;
import java.util.List;
import org.merlin.config.Config;

public class MiruSeaAnomalyMain {

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
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(serviceStartupHealthCheck);
            deployable.buildManageServer().start();

            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);

            MiruSeaAnomalyServiceConfig seaAnomalyServiceConfig = deployable.config(MiruSeaAnomalyServiceConfig.class);

            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new GuavaModule());

            OrderIdProvider orderIdProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceConfig.getInstanceName()));

            serviceStartupHealthCheck.info("building request helpers...", null);
            RequestHelper[] miruReaders = RequestHelperUtil.buildRequestHelpers(seaAnomalyServiceConfig.getMiruReaderHosts(), mapper);
            RequestHelper[] miruWrites = RequestHelperUtil.buildRequestHelpers(seaAnomalyServiceConfig.getMiruWriterHosts(), mapper);

            SampleTrawl logMill = new SampleTrawl(orderIdProvider);

            MiruSeaAnomalyIntakeConfig intakeConfig = deployable.config(MiruSeaAnomalyIntakeConfig.class);
            MiruSeaAnomalyIntakeService inTakeService = new MiruSeaAnomalyIntakeInitializer().initialize(intakeConfig,
                logMill,
                miruWrites,
                miruReaders);

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

            List<MiruManagePlugin> plugins = Lists.newArrayList(
                new MiruManagePlugin("eye-open", "Status", "/seaAnomaly/status",
                    SeaAnomalyStatusPluginEndpoints.class,
                    new SeaAnomalyStatusPluginRegion("soy.sea.anomaly.page.seaAnomalyStatusPluginRegion", renderer, logMill)),
                new MiruManagePlugin("stats", "Trends", "/seaAnomaly/trends",
                    SeaAnomalyTrendsPluginEndpoints.class,
                    new SeaAnomalyTrendsPluginRegion("soy.sea.anomaly.page.seaAnomalyTrendsPluginRegion", renderer, miruReaders)),
                new MiruManagePlugin("search", "Query", "/seaAnomaly/query",
                    SeaAnomalyQueryPluginEndpoints.class,
                    new SeaAnomalyQueryPluginRegion("soy.sea.anomaly.page.seaAnomalyQueryPluginRegion", renderer, miruReaders)));

            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setContext("/static");

            deployable.addEndpoints(MiruSeaAnomalyIntakeEndpoints.class);
            deployable.addInjectables(MiruSeaAnomalyIntakeService.class, inTakeService);

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
