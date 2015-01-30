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
package com.jivesoftware.os.miru.lumberyard.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
import com.jivesoftware.os.miru.lumberyard.deployable.MiruLumberyardIntakeInitializer.MiruLumberyardIntakeConfig;
import com.jivesoftware.os.miru.lumberyard.deployable.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.lumberyard.deployable.analytics.AnalyticsPluginEndpoints;
import com.jivesoftware.os.miru.lumberyard.deployable.region.AnalyticsPluginRegion;
import com.jivesoftware.os.miru.lumberyard.deployable.region.MiruManagePlugin;
import com.jivesoftware.os.server.http.jetty.jersey.server.util.Resource;
import com.jivesoftware.os.upena.main.Deployable;
import com.jivesoftware.os.upena.main.InstanceConfig;
import java.io.File;
import java.util.List;

public class MiruLumberyardMain {

    public static void main(String[] args) throws Exception {
        new MiruLumberyardMain().run(args);
    }

    public void run(String[] args) throws Exception {

        Deployable deployable = new Deployable(args);
        deployable.buildStatusReporter(null).start();
        deployable.buildManageServer().start();

        InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);

        ObjectMapper schemaMapper = new ObjectMapper();
        schemaMapper.registerModule(new GuavaModule());
        MiruLumberyardServiceConfig lumberyardServiceConfig = deployable.config(MiruLumberyardServiceConfig.class);

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());
        RequestHelper[] miruReaders = RequestHelperUtil.buildRequestHelpers(lumberyardServiceConfig.getMiruReaderHosts(), mapper);
        RequestHelper[] miruWrites = RequestHelperUtil.buildRequestHelpers(lumberyardServiceConfig.getMiruWriterHosts(), mapper);

        MiruLumberyardIntakeConfig intakeConfig = deployable.config(MiruLumberyardIntakeConfig.class);
        MiruLumberyardIntakeService inTakeService = new MiruLumberyardIntakeInitializer().initialize(intakeConfig, miruWrites, miruReaders);

        MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);
        MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);
        MiruQueryLumberyardService queryService = new MiruQueryLumberyardInitializer().initialize(renderer);

        List<MiruManagePlugin> plugins = Lists.newArrayList(
            new MiruManagePlugin("Analytics",
                "/miru/lumberyard/analytics",
                AnalyticsPluginEndpoints.class,
                new AnalyticsPluginRegion("soy.miru.page.analyticsPluginRegion", renderer, miruReaders)));

        File staticResourceDir = new File(System.getProperty("user.dir"));
        System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
        Resource sourceTree = new Resource(staticResourceDir)
            //.addResourcePath("../../../../../src/main/resources") // fluff?
            .addResourcePath(rendererConfig.getPathToStaticResources())
            .setContext("/static");

        deployable.addEndpoints(MiruLumberyardIntakeEndpoints.class);
        deployable.addInjectables(MiruLumberyardIntakeService.class, inTakeService);

        deployable.addEndpoints(MiruQueryLumberyardEndpoints.class);
        deployable.addInjectables(MiruQueryLumberyardService.class, queryService);

        for (MiruManagePlugin plugin : plugins) {
            queryService.registerPlugin(plugin);
            deployable.addEndpoints(plugin.endpointsClass);
            deployable.addInjectables(plugin.region.getClass(), plugin.region);
        }

        deployable.addResource(sourceTree);
        deployable.buildServer().start();

    }
}
