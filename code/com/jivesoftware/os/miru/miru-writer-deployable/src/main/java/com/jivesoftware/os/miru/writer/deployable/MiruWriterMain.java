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
package com.jivesoftware.os.miru.writer.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.RowChanges;
import com.jivesoftware.os.amza.shared.RowsChanged;
import com.jivesoftware.os.amza.shared.WALKey;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.jive.utils.health.api.HealthCheckConfigBinder;
import com.jivesoftware.os.jive.utils.health.api.HealthCheckRegistry;
import com.jivesoftware.os.jive.utils.health.api.HealthChecker;
import com.jivesoftware.os.jive.utils.health.api.HealthFactory;
import com.jivesoftware.os.jive.utils.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.jive.utils.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfig;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.topology.MiruRegistryConfig;
import com.jivesoftware.os.miru.cluster.client.MiruClusterClientInitializer;
import com.jivesoftware.os.miru.cluster.client.MiruReplicaSetDirector;
import com.jivesoftware.os.miru.logappender.MiruLogAppender;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSampler;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer.MiruMetricSamplerConfig;
import com.jivesoftware.os.miru.wal.MiruWALDirector;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.miru.wal.activity.ForkingActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.amza.AmzaActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruRCVSActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruRCVSActivityWALWriter;
import com.jivesoftware.os.miru.wal.lookup.MiruRCVSWALLookup;
import com.jivesoftware.os.miru.wal.lookup.MiruWALLookup;
import com.jivesoftware.os.miru.wal.partition.AmzaPartitionIdProvider;
import com.jivesoftware.os.miru.wal.partition.AmzaServiceInitializer;
import com.jivesoftware.os.miru.wal.partition.AmzaServiceInitializer.AmzaServiceConfig;
import com.jivesoftware.os.miru.wal.partition.MiruPartitionIdProvider;
import com.jivesoftware.os.miru.wal.partition.MiruRCVSPartitionIdProvider;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALWriter;
import com.jivesoftware.os.miru.wal.readtracking.MiruWriteToReadTrackingAndSipWAL;
import com.jivesoftware.os.miru.writer.deployable.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.writer.deployable.base.MiruActivityIngress;
import com.jivesoftware.os.miru.writer.deployable.base.MiruLiveIngressActivitySenderProvider;
import com.jivesoftware.os.miru.writer.deployable.base.MiruWarmActivitySenderProvider;
import com.jivesoftware.os.miru.writer.deployable.endpoints.MiruIngressEndpoints;
import com.jivesoftware.os.miru.writer.deployable.endpoints.MiruWALEndpoints;
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
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.merlin.config.Config;

public class MiruWriterMain {

    public static void main(String[] args) throws Exception {
        new MiruWriterMain().run(args);
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

            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(
                new HasUI.UI("Tail", "manage", "/manage/tail"),
                new HasUI.UI("Thead Dump", "manage", "/manage/threadDump"),
                new HasUI.UI("Health", "manage", "/manage/ui"),
                new HasUI.UI("Miru-Writer", "main", "/miru/writer"),
                new HasUI.UI("Miru-Writer-Amza", "main", "/amza"))));

            deployable.buildStatusReporter(null).start();
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(serviceStartupHealthCheck);
            deployable.buildManageServer().start();

            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);

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

            MiruClientConfig clientConfig = deployable.config(MiruClientConfig.class);

            TenantsServiceConnectionDescriptorProvider connections = deployable.getTenantRoutingProvider().getConnections("miru-manage", // TODO expose to conf
                "main");
            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = new TenantRoutingHttpClientInitializer<>();
            TenantAwareHttpClient<String> miruManageClient = tenantRoutingHttpClientInitializer.initialize(connections);

            // TODO add fall back to config
            //MiruClusterClientConfig clusterClientConfig = deployable.config(MiruClusterClientConfig.class);
            MiruStats miruStats = new MiruStats();
            MiruClusterClient clusterClient = new MiruClusterClientInitializer().initialize(miruStats, "", miruManageClient, mapper);

            MiruReplicaSetDirector replicaSetDirector = new MiruReplicaSetDirector(
                new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceConfig.getInstanceName())),
                clusterClient);

            MiruWALInitializer.MiruWAL wal = new MiruWALInitializer().initialize(instanceConfig.getClusterName(), rowColumnValueStoreInitializer, mapper);

            ExecutorService sendActivitiesToHostsThreadPool = Executors.newFixedThreadPool(clientConfig.getSendActivitiesThreadPoolSize());

            Collection<HttpClientConfiguration> configurations = Lists.newArrayList();
            HttpClientConfig baseConfig = HttpClientConfig.newBuilder() // TODO refactor so this is passed in.
                .setSocketTimeoutInMillis(clientConfig.getSocketTimeoutInMillis())
                .setMaxConnections(clientConfig.getMaxConnections())
                .build();
            configurations.add(baseConfig);
            HttpClientFactory httpClientFactory = new HttpClientFactoryProvider().createHttpClientFactory(configurations);

            MiruActivitySenderProvider activitySenderProvider;
            if (clientConfig.getLiveIngress()) {
                activitySenderProvider = new MiruLiveIngressActivitySenderProvider(httpClientFactory, new ObjectMapper());
            } else {
                activitySenderProvider = new MiruWarmActivitySenderProvider(httpClientFactory, new ObjectMapper());
            }

            final Map<MiruTenantId, Boolean> latestAlignmentCache;
            if (clientConfig.getPartitionIdProviderType().equals("rcvs")) {
                latestAlignmentCache = CacheBuilder.newBuilder() // TODO config
                    .maximumSize(clientConfig.getTopologyCacheSize())
                    .expireAfterWrite(1, TimeUnit.MINUTES)
                    .<MiruTenantId, Boolean>build()
                    .asMap();
            } else if (clientConfig.getPartitionIdProviderType().equals("amza")) {
                latestAlignmentCache = Maps.newConcurrentMap();
            } else {
                throw new IllegalStateException("Invalid cluster registry type: " + registryConfig.getClusterRegistryType());
            }

            AmzaService amzaService = null;
            AmzaServiceConfig amzaServiceConfig = deployable.config(AmzaServiceConfig.class);
            if (clientConfig.getPartitionIdProviderType().equals("amza")
                || clientConfig.getActivityWALType().equals("amza")
                || clientConfig.getActivityWALType().equals("fork")) {
                amzaService = new AmzaServiceInitializer().initialize(deployable,
                    instanceConfig.getInstanceName(),
                    instanceConfig.getHost(),
                    instanceConfig.getMainPort(),
                    "miru-wal-" + instanceConfig.getClusterName(),
                    amzaServiceConfig,
                    new RowChanges() {
                        @Override
                        public void changes(RowsChanged changes) throws Exception {
                            if (changes.getRegionName().equals(AmzaPartitionIdProvider.LATEST_PARTITIONS_REGION_NAME)) {
                                for (WALKey key : changes.getApply().columnKeySet()) {
                                    MiruTenantId tenantId = AmzaPartitionIdProvider.extractTenantForLatestPartition(key);
                                    latestAlignmentCache.remove(tenantId);
                                }
                            }
                        }
                    });
            }

            MiruActivityWALWriter activityWALWriter;
            if (clientConfig.getActivityWALType().equals("rcvs")) {
                activityWALWriter = new MiruRCVSActivityWALWriter(wal.getActivityWAL(), wal.getActivitySipWAL());
            } else if (clientConfig.getActivityWALType().equals("amza")) {
                WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(new PrimaryIndexDescriptor("berkeleydb", 0, false, null),
                    null, 1000, 1000);
                activityWALWriter = new AmzaActivityWALWriter(amzaService, storageDescriptor, storageDescriptor, 2, 1, mapper,
                    amzaServiceConfig.getReplicationFactor(), amzaServiceConfig.getTakeFromFactor());
            } else if (clientConfig.getActivityWALType().equals("fork")) {
                MiruRCVSActivityWALWriter rcvsWriter = new MiruRCVSActivityWALWriter(wal.getActivityWAL(), wal.getActivitySipWAL());
                WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(new PrimaryIndexDescriptor("berkeleydb", 0, false, null),
                    null, 1000, 1000);
                AmzaActivityWALWriter amzaWriter = new AmzaActivityWALWriter(amzaService, storageDescriptor, storageDescriptor, 2, 1, mapper,
                    amzaServiceConfig.getReplicationFactor(), amzaServiceConfig.getTakeFromFactor());
                activityWALWriter = new ForkingActivityWALWriter(rcvsWriter, amzaWriter);
            } else {
                throw new IllegalStateException("Invalid activity WAL type: " + clientConfig.getActivityWALType());
            }

            MiruActivityWALReader activityWALReader = new MiruRCVSActivityWALReader(wal.getActivityWAL(), wal.getActivitySipWAL());
            MiruReadTrackingWALWriter readTrackingWALWriter = new MiruWriteToReadTrackingAndSipWAL(wal.getReadTrackingWAL(), wal.getReadTrackingSipWAL());
            MiruReadTrackingWALReader readTrackingWALReader = new MiruReadTrackingWALReaderImpl(wal.getReadTrackingWAL(), wal.getReadTrackingSipWAL());
            MiruWALLookup walLookup = new MiruRCVSWALLookup(wal.getActivityLookupTable(), wal.getRangeLookupTable());

            MiruPartitionIdProvider miruPartitionIdProvider;
            if (clientConfig.getPartitionIdProviderType().equals("rcvs")) {
                miruPartitionIdProvider = new MiruRCVSPartitionIdProvider(clientConfig.getTotalCapacity(),
                    wal.getWriterPartitionRegistry(),
                    activityWALReader);
            } else if (clientConfig.getPartitionIdProviderType().equals("amza")) {
                WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(new PrimaryIndexDescriptor("berkeleydb", 0, false, null),
                    null, 1000, 1000);
                miruPartitionIdProvider = new AmzaPartitionIdProvider(amzaService,
                    storageDescriptor,
                    clientConfig.getTotalCapacity(),
                    activityWALReader);
            } else {
                throw new IllegalStateException("Invalid cluster registry type: " + registryConfig.getClusterRegistryType());
            }

            MiruPartitioner miruPartitioner = new MiruPartitioner(instanceConfig.getInstanceName(),
                miruPartitionIdProvider,
                activityWALWriter,
                activityWALReader,
                readTrackingWALWriter,
                walLookup,
                clientConfig.getPartitionMaximumAgeInMillis());

            MiruActivityIngress activityIngress = new MiruActivityIngress(sendActivitiesToHostsThreadPool,
                clusterClient,
                replicaSetDirector,
                activitySenderProvider,
                miruPartitioner,
                latestAlignmentCache,
                clientConfig.getTopologyCacheSize(),
                clientConfig.getTopologyCacheExpiresInMillis());

            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);
            MiruWALDirector miruWALDirector = new MiruWALDirector(walLookup,
                activityWALReader,
                activityWALWriter,
                miruPartitionIdProvider,
                readTrackingWALReader);

            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                //.addResourcePath("../../../../../src/main/resources") // fluff?
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setContext("/static");

            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);

            MiruWriterUIService miruWriterUIService = new MiruWriterUIServiceInitializer()
                .initialize(renderer, miruWALDirector, activityWALReader, miruStats);

            deployable.addEndpoints(MiruWriterEndpoints.class);
            deployable.addInjectables(MiruWriterUIService.class, miruWriterUIService);
            deployable.addInjectables(MiruWALDirector.class, miruWALDirector);

            deployable.addEndpoints(MiruWALEndpoints.class);

            deployable.addEndpoints(MiruIngressEndpoints.class);
            deployable.addInjectables(MiruActivityIngress.class, activityIngress);
            deployable.addInjectables(MiruStats.class, miruStats);
            deployable.addEndpoints(MiruWriterConfigEndpoints.class);

            deployable.addResource(sourceTree);
            deployable.buildServer().start();
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
