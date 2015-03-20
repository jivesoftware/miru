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
package com.jivesoftware.os.miru.reader.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Charsets;
import com.google.common.collect.Interners;
import com.jivesoftware.os.jive.utils.health.api.HealthCheckConfigBinder;
import com.jivesoftware.os.jive.utils.health.api.HealthCheckRegistry;
import com.jivesoftware.os.jive.utils.health.api.HealthChecker;
import com.jivesoftware.os.jive.utils.health.api.HealthFactory;
import com.jivesoftware.os.jive.utils.health.checkers.DirectBufferHealthChecker;
import com.jivesoftware.os.jive.utils.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.jive.utils.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.cluster.client.ClusterSchemaProvider;
import com.jivesoftware.os.miru.cluster.client.MiruClusterClientInitializer;
import com.jivesoftware.os.miru.logappender.MiruLogAppender;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSampler;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer.MiruMetricSamplerConfig;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.bitmap.SingleBitmapsProvider;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruBackfillerizerInitializer;
import com.jivesoftware.os.miru.plugin.index.MiruJustInTimeBackfillerizer;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.reader.deployable.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.MiruServiceInitializer;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.service.endpoint.MiruReaderEndpoints;
import com.jivesoftware.os.miru.service.endpoint.MiruWriterEndpoints;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocatorInitializer;
import com.jivesoftware.os.miru.wal.client.MiruWALClientInitializer;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import org.merlin.config.Config;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

public class MiruReaderMain {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static void main(String[] args) throws Exception {
        new MiruReaderMain().run(args);
    }

    public void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            final Deployable deployable = new Deployable(args);

            HealthFactory.initialize(
                new HealthCheckConfigBinder() {
                    @Override
                    public <C extends Config> C bindConfig(Class<C> configurationInterfaceClass) {
                        return deployable.config(configurationInterfaceClass);
                    }
                },
                new HealthCheckRegistry() {
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
                new HasUI.UI("Miru-Reader", "main", "/"))));
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(new DirectBufferHealthChecker(deployable.config(DirectBufferHealthChecker.DirectBufferHealthCheckerConfig.class)));
            deployable.addHealthCheck(serviceStartupHealthCheck);
            deployable.buildManageServer().start();

            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            mapper.registerModule(new GuavaModule());

            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);

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

            MiruHost miruHost = new MiruHost(instanceConfig.getHost(), instanceConfig.getMainPort());

            MiruServiceConfig miruServiceConfig = deployable.config(MiruServiceConfig.class);

            HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
                .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList());

            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = new TenantRoutingHttpClientInitializer<>();
            TenantAwareHttpClient<String> miruWriterClient = tenantRoutingHttpClientInitializer.initialize(deployable
                .getTenantRoutingProvider()
                .getConnections("miru-writer", "main")); // TODO expose to conf

            MiruWALClient walClient = new MiruWALClientInitializer().initialize("", miruWriterClient, mapper, 10_000);

            MiruLifecyle<MiruJustInTimeBackfillerizer> backfillerizerLifecycle = new MiruBackfillerizerInitializer()
                .initialize(miruServiceConfig.getReadStreamIdsPropName(), miruHost, walClient);

            backfillerizerLifecycle.start();
            final MiruJustInTimeBackfillerizer backfillerizer = backfillerizerLifecycle.getService();

            MiruResourceLocator miruResourceLocator = new MiruResourceLocatorInitializer().initialize(miruServiceConfig);

            final MiruTermComposer termComposer = new MiruTermComposer(Charsets.UTF_8);
            final MiruActivityInternExtern internExtern = new MiruActivityInternExtern(
                Interners.<MiruIBA>newWeakInterner(),
                Interners.<MiruTermId>newWeakInterner(),
                Interners.<MiruTenantId>newStrongInterner(),
                // makes sense to share string internment as this is authz in both cases
                Interners.<String>newWeakInterner(),
                termComposer);

            final MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();

            TenantAwareHttpClient<String> miruManageClient = tenantRoutingHttpClientInitializer.initialize(deployable
                .getTenantRoutingProvider()
                .getConnections("miru-manage", "main"));  // TODO expose to conf

            // TODO add fall back to config
            MiruClusterClient clusterClient = new MiruClusterClientInitializer().initialize("", miruManageClient, mapper);

            MiruSchemaProvider miruSchemaProvider = new ClusterSchemaProvider(clusterClient, 10000); // TODO config

            MiruLifecyle<MiruService> miruServiceLifecyle = new MiruServiceInitializer().initialize(miruServiceConfig,
                clusterClient,
                miruHost,
                miruSchemaProvider,
                walClient,
                httpClientFactory,
                miruResourceLocator,
                termComposer,
                internExtern,
                new SingleBitmapsProvider<>(bitmaps));

            miruServiceLifecyle.start();
            final MiruService miruService = miruServiceLifecyle.getService();

            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);

            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setContext("/static");

            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);
            MiruReaderUIService uiService = new MiruReaderUIInitializer().initialize(renderer);

            deployable.addEndpoints(MiruReaderUIEndpoints.class);
            deployable.addInjectables(MiruReaderUIService.class, uiService);

            deployable.addEndpoints(MiruWriterEndpoints.class);
            deployable.addEndpoints(MiruReaderEndpoints.class);
            deployable.addInjectables(MiruService.class, miruService);

            MiruProvider<Miru> miruProvider = new MiruProvider<Miru>() {
                @Override
                public Miru getMiru(MiruTenantId tenantId) {
                    return miruService;
                }

                @Override
                public MiruActivityInternExtern getActivityInternExtern(MiruTenantId tenantId) {
                    return internExtern;
                }

                @Override
                public MiruJustInTimeBackfillerizer getBackfillerizer(MiruTenantId tenantId) {
                    return backfillerizer;
                }

                @Override
                public MiruTermComposer getTermComposer() {
                    return termComposer;
                }
            };

            for (String pluginPackage : miruServiceConfig.getPluginPackages().split(",")) {
                Reflections reflections = new Reflections(new ConfigurationBuilder()
                    .setUrls(ClasspathHelper.forPackage(pluginPackage.trim()))
                    .setScanners(new SubTypesScanner(), new TypesScanner()));
                Set<Class<? extends MiruPlugin>> pluginTypes = reflections.getSubTypesOf(MiruPlugin.class);
                for (Class<? extends MiruPlugin> pluginType : pluginTypes) {
                    LOG.info("Loading plugin {}", pluginType.getSimpleName());
                    add(miruProvider, deployable, pluginType.newInstance());
                }
            }

            deployable.addEndpoints(MiruReaderConfigEndpoints.class);
            deployable.addResource(sourceTree);

            deployable.buildServer().start();
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }

    private <E, I> void add(MiruProvider<? extends Miru> miruProvider, Deployable deployable, MiruPlugin<E, I> plugin) {
        Class<E> endpointsClass = plugin.getEndpointsClass();
        deployable.addEndpoints(endpointsClass);
        Collection<MiruEndpointInjectable<I>> injectables = plugin.getInjectables(miruProvider);
        for (MiruEndpointInjectable<?> miruEndpointInjectable : injectables) {
            deployable.addInjectables(miruEndpointInjectable.getInjectableClass(), miruEndpointInjectable.getInjectable());
        }
    }
}
