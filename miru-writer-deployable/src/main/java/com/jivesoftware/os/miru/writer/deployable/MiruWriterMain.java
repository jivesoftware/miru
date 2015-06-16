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
import com.google.common.collect.Maps;
import com.jivesoftware.os.amza.client.AmzaKretrProvider;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.partition.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.wal.WALKey;
import com.jivesoftware.os.amza.shared.wal.WALStorageDescriptor;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceConfig;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceInitializer;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.AmzaCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALConfig;
import com.jivesoftware.os.miru.api.wal.RCVSCursor;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.logappender.MiruLogAppender;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSampler;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer.MiruMetricSamplerConfig;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.wal.client.MiruWALClientInitializer;
import com.jivesoftware.os.miru.writer.deployable.base.MiruActivityIngress;
import com.jivesoftware.os.miru.writer.deployable.endpoints.MiruIngressEndpoints;
import com.jivesoftware.os.miru.writer.partition.AmzaPartitionIdProvider;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckRegistry;
import com.jivesoftware.os.routing.bird.health.api.HealthChecker;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.http.client.TenantRoutingHttpClientInitializer;
import com.jivesoftware.os.routing.bird.server.util.Resource;
import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public class MiruWriterMain {

    public static void main(String[] args) throws Exception {
        new MiruWriterMain().run(args);
    }

    public interface WriterAmzaServiceConfig extends MiruAmzaServiceConfig {

        @StringDefault("./var/amza/writer/data/")
        @Override
        String getWorkingDirectories();

        @StringDefault("./var/amza/writer/index/")
        @Override
        String getIndexDirectories();

        @StringDefault("225.5.6.24")
        @Override
        String getAmzaDiscoveryGroup();

        @IntDefault(1224)
        @Override
        int getAmzaDiscoveryPort();

        @IntDefault(1)
        int getReplicateCursorQuorum();

        @LongDefault(60_000L)
        long getReplicateCursorTimeoutMillis();
    }

    public void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            final Deployable deployable = new Deployable(args);
            HealthFactory.initialize(deployable::config,
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
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(
                new HasUI.UI("Reset Errors", "manage", "/manage/resetErrors"),
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

            MiruClientConfig clientConfig = deployable.config(MiruClientConfig.class);

            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = new TenantRoutingHttpClientInitializer<>();
            TenantAwareHttpClient<String> manageHttpClient = tenantRoutingHttpClientInitializer.initialize(deployable
                .getTenantRoutingProvider()
                .getConnections("miru-manage", "main")); // TODO expose to conf

            TenantAwareHttpClient<String> walHttpClient = tenantRoutingHttpClientInitializer.initialize(deployable
                .getTenantRoutingProvider()
                .getConnections("miru-wal", "main")); // TODO expose to conf

            final Map<MiruTenantId, Boolean> latestAlignmentCache = Maps.newConcurrentMap();

            WriterAmzaServiceConfig miruAmzaServiceConfig = deployable.config(WriterAmzaServiceConfig.class);
            AmzaService amzaService = new MiruAmzaServiceInitializer().initialize(deployable,
                instanceConfig.getInstanceName(),
                instanceConfig.getInstanceKey(),
                instanceConfig.getHost(),
                instanceConfig.getMainPort(),
                "miru-writer-" + instanceConfig.getClusterName(),
                miruAmzaServiceConfig,
                changes -> {
                    if (changes.getVersionedPartitionName().getPartitionName().equals(AmzaPartitionIdProvider.LATEST_PARTITIONS_PARTITION_NAME)) {
                        for (WALKey key : changes.getApply().columnKeySet()) {
                            MiruTenantId tenantId = AmzaPartitionIdProvider.extractTenantForLatestPartition(key);
                            latestAlignmentCache.remove(tenantId);
                        }
                    }
                });

            MiruWALConfig walConfig = deployable.config(MiruWALConfig.class);
            MiruWALClient<?, ?> walClient;
            if (walConfig.getActivityWALType().equals("rcvs") || walConfig.getActivityWALType().equals("rcvs_amza")) {
                MiruWALClient<RCVSCursor, RCVSSipCursor> rcvsWALClient = new MiruWALClientInitializer().initialize("", walHttpClient, mapper, 10_000,
                    "/miru/wal/rcvs", RCVSCursor.class, RCVSSipCursor.class);
                walClient = rcvsWALClient;
            } else if (walConfig.getActivityWALType().equals("amza") || walConfig.getActivityWALType().equals("amza_rcvs")) {
                MiruWALClient<AmzaCursor, AmzaSipCursor> amzaWALClient = new MiruWALClientInitializer().initialize("", walHttpClient, mapper, 10_000,
                    "/miru/wal/amza", AmzaCursor.class, AmzaSipCursor.class);
                walClient = amzaWALClient;
            } else {
                throw new IllegalStateException("Invalid activity WAL type: " + walConfig.getActivityWALType());
            }

            WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(new PrimaryIndexDescriptor("berkeleydb", 0, false, null),
                null, 1000, 1000);

            AmzaKretrProvider amzaKretrProvider = new AmzaKretrProvider(amzaService);
            AmzaPartitionIdProvider amzaPartitionIdProvider = new AmzaPartitionIdProvider(amzaService,
                amzaKretrProvider,
                miruAmzaServiceConfig.getReplicateCursorQuorum(),
                miruAmzaServiceConfig.getReplicateCursorTimeoutMillis(),
                storageDescriptor,
                clientConfig.getTotalCapacity(),
                walClient);

            MiruPartitioner miruPartitioner = new MiruPartitioner(instanceConfig.getInstanceName(),
                amzaPartitionIdProvider,
                walClient,
                clientConfig.getPartitionMaximumAgeInMillis());

            ExecutorService sendActivitiesExecutorService = Executors.newFixedThreadPool(clientConfig.getSendActivitiesThreadPoolSize());
            MiruActivityIngress activityIngress = new MiruActivityIngress(miruPartitioner, latestAlignmentCache, sendActivitiesExecutorService);

            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);

            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                //.addResourcePath("../../../../../src/main/resources") // fluff?
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setContext("/static");

            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);

            MiruStats miruStats = new MiruStats();
            MiruWriterUIService miruWriterUIService = new MiruWriterUIServiceInitializer()
                .initialize(renderer, miruStats);

            deployable.addEndpoints(MiruWriterEndpoints.class);
            deployable.addInjectables(MiruWriterUIService.class, miruWriterUIService);

            deployable.addEndpoints(MiruIngressEndpoints.class);
            deployable.addInjectables(MiruStats.class, miruStats);
            deployable.addInjectables(MiruActivityIngress.class, activityIngress);

            deployable.addResource(sourceTree);
            deployable.buildServer().start();
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
