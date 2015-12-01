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
import com.google.common.collect.Maps;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.wal.AmzaCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALConfig;
import com.jivesoftware.os.miru.api.wal.RCVSCursor;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.cluster.client.ClusterSchemaProvider;
import com.jivesoftware.os.miru.cluster.client.MiruClusterClientInitializer;
import com.jivesoftware.os.miru.logappender.MiruLogAppender;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.logappender.RoutingBirdLogSenderProvider;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSampler;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer.MiruMetricSamplerConfig;
import com.jivesoftware.os.miru.metric.sampler.RoutingBirdMetricSampleSenderProvider;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.backfill.AmzaInboxReadTracker;
import com.jivesoftware.os.miru.plugin.backfill.MiruInboxReadTracker;
import com.jivesoftware.os.miru.plugin.backfill.MiruJustInTimeBackfillerizer;
import com.jivesoftware.os.miru.plugin.backfill.RCVSInboxReadTracker;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.SingleBitmapsProvider;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruBackfillerizerInitializer;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.marshaller.AmzaSipIndexMarshaller;
import com.jivesoftware.os.miru.plugin.marshaller.RCVSSipIndexMarshaller;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.plugin.query.LuceneBackedQueryParser;
import com.jivesoftware.os.miru.plugin.query.MiruQueryParser;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.MiruServiceInitializer;
import com.jivesoftware.os.miru.service.endpoint.MiruReaderEndpoints;
import com.jivesoftware.os.miru.service.endpoint.MiruWriterEndpoints;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocatorInitializer;
import com.jivesoftware.os.miru.service.partition.AmzaSipTrackerFactory;
import com.jivesoftware.os.miru.service.partition.PartitionErrorTracker;
import com.jivesoftware.os.miru.service.partition.RCVSSipTrackerFactory;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.wal.client.MiruWALClientInitializer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckRegistry;
import com.jivesoftware.os.routing.bird.health.api.HealthChecker;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.checkers.DirectBufferHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.routing.bird.http.client.ConnectionDescriptorSelectiveStrategy;
import com.jivesoftware.os.routing.bird.http.client.HttpDeliveryClientHealthProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.http.client.TenantRoutingHttpClientInitializer;
import com.jivesoftware.os.routing.bird.server.util.Resource;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;
import com.jivesoftware.os.routing.bird.shared.TenantsServiceConnectionDescriptorProvider;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
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
                deployable::config,
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
            deployable.addErrorHealthChecks();
            deployable.buildStatusReporter(null).start();
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(new HasUI.UI("manage", "manage", "/manage/ui"),
                new HasUI.UI("Reset Errors", "manage", "/manage/resetErrors"),
                new HasUI.UI("Tail", "manage", "/manage/tail"),
                new HasUI.UI("Thread Dump", "manage", "/manage/threadDump"),
                new HasUI.UI("Health", "manage", "/manage/ui"),
                new HasUI.UI("Miru-Reader", "main", "/"))));
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(new DirectBufferHealthChecker(deployable.config(DirectBufferHealthChecker.DirectBufferHealthCheckerConfig.class)));
            deployable.addHealthCheck(serviceStartupHealthCheck);
            deployable.buildManageServer().start();

            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            mapper.registerModule(new GuavaModule());

            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);

            TenantRoutingProvider tenantRoutingProvider = deployable.getTenantRoutingProvider();
            MiruLogAppenderInitializer.MiruLogAppenderConfig miruLogAppenderConfig = deployable.config(MiruLogAppenderInitializer.MiruLogAppenderConfig.class);
            TenantsServiceConnectionDescriptorProvider logConnections = tenantRoutingProvider.getConnections("miru-stumptown", "main");
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
            TenantsServiceConnectionDescriptorProvider metricConnections = tenantRoutingProvider.getConnections("miru-anomaly", "main");
            MiruMetricSampler sampler = new MiruMetricSamplerInitializer().initialize(null, //TODO datacenter
                instanceConfig.getClusterName(),
                instanceConfig.getHost(),
                instanceConfig.getServiceName(),
                String.valueOf(instanceConfig.getInstanceName()),
                instanceConfig.getVersion(),
                metricSamplerConfig,
                new RoutingBirdMetricSampleSenderProvider<>(metricConnections, "", miruLogAppenderConfig.getSocketTimeoutInMillis()));
            sampler.start();

            MiruHost miruHost = new MiruHost(instanceConfig.getHost(), instanceConfig.getMainPort());

            MiruServiceConfig miruServiceConfig = deployable.config(MiruServiceConfig.class);
            MiruWALConfig walConfig = deployable.config(MiruWALConfig.class);

            MiruResourceLocator miruResourceLocator = new MiruResourceLocatorInitializer().initialize(miruServiceConfig);

            final MiruTermComposer termComposer = new MiruTermComposer(Charsets.UTF_8);
            final MiruActivityInternExtern internExtern = new MiruActivityInternExtern(
                Interners.<MiruIBA>newWeakInterner(),
                Interners.<MiruTermId>newWeakInterner(),
                Interners.<MiruTenantId>newStrongInterner(),
                // makes sense to share string internment as this is authz in both cases
                Interners.<String>newWeakInterner(),
                termComposer);

            MiruBitmaps<?, ?> bitmaps = miruServiceConfig.getBitmapsClass().newInstance();

            HttpDeliveryClientHealthProvider clientHealthProvider = new HttpDeliveryClientHealthProvider(instanceConfig.getInstanceKey(),
                HttpRequestHelperUtils.buildRequestHelper(instanceConfig.getRoutesHost(), instanceConfig.getRoutesPort()),
                instanceConfig.getConnectionsHealth(), 5_000, 100);

            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = new TenantRoutingHttpClientInitializer<>();
            TenantAwareHttpClient<String> walHttpClient = tenantRoutingHttpClientInitializer.initialize(tenantRoutingProvider
                    .getConnections("miru-wal", "main"),
                clientHealthProvider,
                10, 10_000); // TODO expose to conf

            TenantAwareHttpClient<String> manageHttpClient = tenantRoutingHttpClientInitializer.initialize(tenantRoutingProvider
                    .getConnections("miru-manage", "main"),
                clientHealthProvider,
                10, 10_000);  // TODO expose to conf

            TenantAwareHttpClient<String> readerHttpClient = tenantRoutingHttpClientInitializer.initialize(tenantRoutingProvider
                    .getConnections("miru-reader", "main"),
                clientHealthProvider,
                10, 10_000);  // TODO expose to conf

            // TODO add fall back to config
            final MiruStats miruStats = new MiruStats();
            MiruClusterClient clusterClient = new MiruClusterClientInitializer().initialize(miruStats, "", manageHttpClient, mapper);
            MiruSchemaProvider miruSchemaProvider = new ClusterSchemaProvider(clusterClient, 10000); // TODO config

            PartitionErrorTracker.PartitionErrorTrackerConfig partitionErrorTrackerConfig = deployable
                .config(PartitionErrorTracker.PartitionErrorTrackerConfig.class);
            PartitionErrorTracker partitionErrorTracker = new PartitionErrorTracker(partitionErrorTrackerConfig);

            deployable.addHealthCheck(partitionErrorTracker);

            MiruInboxReadTracker inboxReadTracker;
            MiruLifecyle<MiruService> miruServiceLifecyle;
            if (walConfig.getActivityWALType().equals("rcvs") || walConfig.getActivityWALType().equals("rcvs_amza")) {
                MiruWALClient<RCVSCursor, RCVSSipCursor> rcvsWALClient = new MiruWALClientInitializer().initialize("", walHttpClient, mapper, 10_000,
                    "/miru/wal/rcvs", RCVSCursor.class, RCVSSipCursor.class);

                inboxReadTracker = new RCVSInboxReadTracker(rcvsWALClient);
                miruServiceLifecyle = new MiruServiceInitializer().initialize(miruServiceConfig,
                    miruStats,
                    clusterClient,
                    miruHost,
                    miruSchemaProvider,
                    rcvsWALClient,
                    new RCVSSipTrackerFactory(),
                    new RCVSSipIndexMarshaller(),
                    miruResourceLocator,
                    termComposer,
                    internExtern,
                    new SingleBitmapsProvider(bitmaps),
                    partitionErrorTracker);
            } else if (walConfig.getActivityWALType().equals("amza") || walConfig.getActivityWALType().equals("amza_rcvs")) {
                MiruWALClient<AmzaCursor, AmzaSipCursor> amzaWALClient = new MiruWALClientInitializer().initialize("", walHttpClient, mapper, 10_000,
                    "/miru/wal/amza", AmzaCursor.class, AmzaSipCursor.class);

                inboxReadTracker = new AmzaInboxReadTracker(amzaWALClient);
                miruServiceLifecyle = new MiruServiceInitializer().initialize(miruServiceConfig,
                    miruStats,
                    clusterClient,
                    miruHost,
                    miruSchemaProvider,
                    amzaWALClient,
                    new AmzaSipTrackerFactory(),
                    new AmzaSipIndexMarshaller(),
                    miruResourceLocator,
                    termComposer,
                    internExtern,
                    new SingleBitmapsProvider(bitmaps),
                    partitionErrorTracker);
            } else {
                throw new IllegalStateException("Invalid activity WAL type: " + walConfig.getActivityWALType());
            }

            MiruLifecyle<MiruJustInTimeBackfillerizer> backfillerizerLifecycle = new MiruBackfillerizerInitializer()
                .initialize(miruServiceConfig.getReadStreamIdsPropName(), miruHost, inboxReadTracker);

            backfillerizerLifecycle.start();
            final MiruJustInTimeBackfillerizer backfillerizer = backfillerizerLifecycle.getService();

            miruServiceLifecyle.start();
            final MiruService miruService = miruServiceLifecyle.getService();

            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);

            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setContext("/static");

            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);
            MiruReaderUIService uiService = new MiruReaderUIInitializer().initialize(instanceConfig.getClusterName(),
                instanceConfig.getInstanceName(),
                renderer,
                miruStats,
                miruService,
                partitionErrorTracker,
                tenantRoutingProvider);

            deployable.addEndpoints(MiruReaderUIEndpoints.class);
            deployable.addInjectables(MiruReaderUIService.class, uiService);
            deployable.addInjectables(MiruStats.class, miruStats);

            deployable.addEndpoints(MiruWriterEndpoints.class);
            deployable.addEndpoints(MiruReaderEndpoints.class);
            deployable.addInjectables(MiruService.class, miruService);

            deployable.addInjectables(ObjectMapper.class, mapper);

            Map<Class<?>, MiruRemotePartition<?, ?, ?>> pluginRemotesMap = Maps.newConcurrentMap();

            Map<MiruHost, ConnectionDescriptorSelectiveStrategy> readerStrategyCache = Maps.newConcurrentMap();

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

                @Override
                public MiruQueryParser getQueryParser(String defaultField) {
                    return new LuceneBackedQueryParser(defaultField);
                }

                @Override
                public MiruStats getStats() {
                    return miruStats;
                }

                @Override
                public <R extends MiruRemotePartition<?, ?, ?>> R getRemotePartition(Class<R> remotePartitionClass) {
                    return (R) pluginRemotesMap.get(remotePartitionClass);
                }

                @Override
                public TenantAwareHttpClient<String> getReaderHttpClient() {
                    return readerHttpClient;
                }

                @Override
                public Map<MiruHost, ConnectionDescriptorSelectiveStrategy> getReaderStrategyCache() {
                    return readerStrategyCache;
                }

                @Override
                public <C extends Config> C getConfig(Class<C> configClass) {
                    return deployable.config(configClass);
                }
            };

            for (String pluginPackage : miruServiceConfig.getPluginPackages().split(",")) {
                Reflections reflections = new Reflections(new ConfigurationBuilder()
                    .setUrls(ClasspathHelper.forPackage(pluginPackage.trim()))
                    .setScanners(new SubTypesScanner(), new TypesScanner()));
                Set<Class<? extends MiruPlugin>> pluginTypes = reflections.getSubTypesOf(MiruPlugin.class);
                for (Class<? extends MiruPlugin> pluginType : pluginTypes) {
                    LOG.info("Loading plugin {}", pluginType.getSimpleName());
                    add(miruProvider, deployable, pluginType.newInstance(), pluginRemotesMap);
                }
            }

            deployable.addEndpoints(MiruReaderConfigEndpoints.class);
            deployable.addResource(sourceTree);

            deployable.buildServer().start();
            clientHealthProvider.start();
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }

    private <E, I> void add(MiruProvider<? extends Miru> miruProvider,
        Deployable deployable,
        MiruPlugin<E, I> plugin,
        Map<Class<?>, MiruRemotePartition<?, ?, ?>> pluginRemotesMap) {
        Class<E> endpointsClass = plugin.getEndpointsClass();
        deployable.addEndpoints(endpointsClass);
        Collection<MiruEndpointInjectable<I>> injectables = plugin.getInjectables(miruProvider);
        for (MiruEndpointInjectable<?> miruEndpointInjectable : injectables) {
            deployable.addInjectables(miruEndpointInjectable.getInjectableClass(), miruEndpointInjectable.getInjectable());
        }
        for (MiruRemotePartition<?, ?, ?> remotePartition : plugin.getRemotePartitions(miruProvider)) {
            pluginRemotesMap.put(remotePartition.getClass(), remotePartition);
        }
    }
}
