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
package com.jivesoftware.os.miru.wal.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.embed.EmbedAmzaServiceInitializer.Lifecycle;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceConfig;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceInitializer;
import com.jivesoftware.os.miru.api.HostPortProvider;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.RoutingBirdHostPortProvider;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.TenantAndPartition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.wal.AmzaCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALConfig;
import com.jivesoftware.os.miru.api.wal.RCVSCursor;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.cluster.client.MiruClusterClientInitializer;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer.MiruLogAppenderConfig;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer.MiruMetricSamplerConfig;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.wal.AmzaWALDirector;
import com.jivesoftware.os.miru.wal.AmzaWALUtil;
import com.jivesoftware.os.miru.wal.MiruWALDirector;
import com.jivesoftware.os.miru.wal.MiruWALRepair;
import com.jivesoftware.os.miru.wal.RCVSWALDirector;
import com.jivesoftware.os.miru.wal.RCVSWALInitializer;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.amza.AmzaActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.amza.AmzaActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.rcvs.RCVSActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.rcvs.RCVSActivityWALWriter;
import com.jivesoftware.os.miru.wal.client.MiruWALClientInitializer;
import com.jivesoftware.os.miru.wal.deployable.endpoints.AmzaWALEndpoints;
import com.jivesoftware.os.miru.wal.deployable.endpoints.RCVSWALEndpoints;
import com.jivesoftware.os.miru.wal.lookup.AmzaWALLookup;
import com.jivesoftware.os.miru.wal.lookup.RCVSWALLookup;
import com.jivesoftware.os.miru.wal.readtracking.amza.AmzaReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.amza.AmzaReadTrackingWALWriter;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.RCVSReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.RCVSReadTrackingWALWriter;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.deployable.DeployableHealthCheckRegistry;
import com.jivesoftware.os.routing.bird.deployable.ErrorHealthCheckConfig;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.endpoints.base.FullyOnlineVersion;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI.UI;
import com.jivesoftware.os.routing.bird.endpoints.base.LoadBalancerHealthCheckEndpoints;
import com.jivesoftware.os.routing.bird.health.api.HealthChecker;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.api.ScheduledMinMaxHealthCheckConfig;
import com.jivesoftware.os.routing.bird.health.checkers.DiskFreeHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.FileDescriptorCountHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCPauseHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.LoadAverageHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreads;
import com.jivesoftware.os.routing.bird.health.checkers.SystemCpuHealthChecker;
import com.jivesoftware.os.routing.bird.http.client.HttpDeliveryClientHealthProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.http.client.TenantRoutingHttpClientInitializer;
import com.jivesoftware.os.routing.bird.server.util.Resource;
import com.jivesoftware.os.routing.bird.shared.BoundedExecutor;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;
import com.jivesoftware.os.routing.bird.shared.TenantsServiceConnectionDescriptorProvider;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.StringUtils;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public class MiruWALMain {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static void main(String[] args) throws Exception {
        new MiruWALMain().run(args);
    }

    public interface DiskFreeCheck extends ScheduledMinMaxHealthCheckConfig {

        @StringDefault("disk>free")
        @Override
        String getName();

        @LongDefault(80)
        @Override
        Long getMax();

    }

    public interface WALAmzaServiceConfig extends MiruAmzaServiceConfig {

        @StringDefault("./var/amza/wal/data/")
        @Override
        String getWorkingDirectories();

        @LongDefault(60_000L)
        long getReplicateTimeoutMillis();
    }

    void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            final Deployable deployable = new Deployable(args);
            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);
            HealthFactory.initialize(deployable::config, new DeployableHealthCheckRegistry(deployable));
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(new UI("Miru-WAL", "main", "/ui"),
                new UI("Miru-WAL-Amza", "main", "/amza/ui"))));

            deployable.addHealthCheck(new GCPauseHealthChecker(deployable.config(GCPauseHealthChecker.GCPauseHealthCheckerConfig.class)));
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(new SystemCpuHealthChecker(deployable.config(SystemCpuHealthChecker.SystemCpuHealthCheckerConfig.class)));
            deployable.addHealthCheck(new LoadAverageHealthChecker(deployable.config(LoadAverageHealthChecker.LoadAverageHealthCheckerConfig.class)));
            deployable.addHealthCheck(
                new FileDescriptorCountHealthChecker(deployable.config(FileDescriptorCountHealthChecker.FileDescriptorCountHealthCheckerConfig.class)));
            deployable.addHealthCheck(serviceStartupHealthCheck);
            deployable.addErrorHealthChecks(deployable.config(ErrorHealthCheckConfig.class));
            AtomicReference<Callable<Boolean>> isAmzaReady = new AtomicReference<>(() -> false);
            deployable.addManageInjectables(FullyOnlineVersion.class, (FullyOnlineVersion) () -> {
                if (serviceStartupHealthCheck.startupHasSucceeded() && isAmzaReady.get().call()) {
                    return instanceConfig.getVersion();
                } else {
                    return null;
                }
            });
            deployable.buildManageServer().start();

            WALAmzaServiceConfig amzaServiceConfig = deployable.config(WALAmzaServiceConfig.class);

            List<File> amzaPaths = Lists.newArrayList(Iterables.transform(
                Splitter.on(',').split(amzaServiceConfig.getWorkingDirectories()),
                input -> new File(input.trim())));
            HealthFactory.scheduleHealthChecker(DiskFreeCheck.class,
                config1 -> (HealthChecker) new DiskFreeHealthChecker(config1, amzaPaths.toArray(new File[amzaPaths.size()])));


            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            mapper.registerModule(new GuavaModule());

            TenantRoutingProvider tenantRoutingProvider = deployable.getTenantRoutingProvider();
            HttpDeliveryClientHealthProvider clientHealthProvider = new HttpDeliveryClientHealthProvider(instanceConfig.getInstanceKey(),
                HttpRequestHelperUtils.buildRequestHelper(false, false, null, instanceConfig.getRoutesHost(), instanceConfig.getRoutesPort()),
                instanceConfig.getConnectionsHealth(), 5_000, 100);
            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = deployable.getTenantRoutingHttpClientInitializer();

            MiruLogAppenderConfig miruLogAppenderConfig = deployable.config(MiruLogAppenderConfig.class);
            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> miruStumptownClient = tenantRoutingHttpClientInitializer.builder(
                tenantRoutingProvider.getConnections(
                    "miru-stumptown",
                    "main",
                    10_000),
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build();
            new MiruLogAppenderInitializer().initialize(
                instanceConfig.getDatacenter(),
                instanceConfig.getClusterName(),
                instanceConfig.getHost(),
                instanceConfig.getServiceName(),
                String.valueOf(instanceConfig.getInstanceName()),
                instanceConfig.getVersion(),
                miruLogAppenderConfig,
                miruStumptownClient).install();

            MiruMetricSamplerConfig metricSamplerConfig = deployable.config(MiruMetricSamplerConfig.class);
            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> miruAnomalyClient = tenantRoutingHttpClientInitializer.builder(
                tenantRoutingProvider.getConnections(
                    "miru-anomaly",
                    "main",
                    10_000),
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build();
            new MiruMetricSamplerInitializer().initialize(
                instanceConfig.getDatacenter(),
                instanceConfig.getClusterName(),
                instanceConfig.getHost(),
                instanceConfig.getServiceName(),
                String.valueOf(instanceConfig.getInstanceName()),
                instanceConfig.getVersion(),
                metricSamplerConfig,
                miruAnomalyClient).start();

            RCVSWALConfig rcvsWALConfig = deployable.config(RCVSWALConfig.class);
            MiruWALConfig walConfig = deployable.config(MiruWALConfig.class);

            RowColumnValueStoreInitializer<? extends Exception> rowColumnValueStoreInitializer = null;
            if (RowColumnValueStoreProvider.class.isAssignableFrom(rcvsWALConfig.getRowColumnValueStoreProviderClass())) {
                RowColumnValueStoreProvider rowColumnValueStoreProvider = rcvsWALConfig.getRowColumnValueStoreProviderClass().newInstance();
                @SuppressWarnings("unchecked")
                RowColumnValueStoreInitializer<? extends Exception> initializer = rowColumnValueStoreProvider
                    .create(deployable.config(rowColumnValueStoreProvider.getConfigurationClass()));
                rowColumnValueStoreInitializer = initializer;
            }

            MiruStats miruStats = new MiruStats();

            Lifecycle amzaLifecycle = new MiruAmzaServiceInitializer().initialize(deployable,
                clientHealthProvider,
                instanceConfig.getInstanceName(),
                instanceConfig.getInstanceKey(),
                instanceConfig.getServiceName(),
                instanceConfig.getDatacenter(),
                instanceConfig.getRack(),
                instanceConfig.getHost(),
                instanceConfig.getMainPort(),
                instanceConfig.getMainServiceAuthEnabled(),
                null,
                amzaServiceConfig,
                true,
                -1,
                changes -> {
                });

            EmbeddedClientProvider clientProvider = new EmbeddedClientProvider(amzaLifecycle.amzaService);
            PartitionProperties activityProperties = new PartitionProperties(Durability.fsync_async, 0, 0, 0, 0, 0, 0, 0, 0,
                false,
                Consistency.leader_quorum,
                true,
                true, //TODO use for ring config? amzaServiceConfig.getTakeFromFactor(),
                false,
                RowType.snappy_primary,
                "lab",
                -1,
                null,
                -1,
                -1);
            PartitionProperties readTrackingProperties = new PartitionProperties(Durability.fsync_async, 0, 0, 0, 0, 0, 0, 0, 0,
                false,
                Consistency.leader_quorum,
                true,
                true, //TODO use for ring config? amzaServiceConfig.getTakeFromFactor(),
                false,
                RowType.snappy_primary,
                "lab",
                -1,
                null,
                -1,
                -1);
            PartitionProperties lookupProperties = new PartitionProperties(Durability.fsync_async, 0, 0, 0, 0, 0, 0, 0, 0,
                false,
                Consistency.quorum,
                true,
                true, //TODO use for ring config? amzaServiceConfig.getTakeFromFactor(),
                false,
                RowType.primary,
                "lab",
                -1,
                null,
                -1,
                -1);
            AmzaWALUtil amzaWALUtil = new AmzaWALUtil(amzaLifecycle.amzaService,
                clientProvider,
                activityProperties,
                readTrackingProperties,
                lookupProperties,
                amzaServiceConfig.getActivityRingSize(),
                amzaServiceConfig.getActivityRoutingTimeoutMillis(),
                amzaServiceConfig.getReadTrackingRingSize(),
                amzaServiceConfig.getReadTrackingRoutingTimeoutMillis());

            TenantsServiceConnectionDescriptorProvider walConnectionDescriptorProvider =
                tenantRoutingProvider.getConnections("miru-wal", "main", 10_000); // TODO config
            @SuppressWarnings("unchecked")
            HostPortProvider hostPortProvider = new RoutingBirdHostPortProvider(walConnectionDescriptorProvider, "");

            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> walHttpClient = tenantRoutingHttpClientInitializer.builder(
                tenantRoutingProvider.getConnections(instanceConfig.getServiceName(), "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build(); // TODO expose to conf

            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> manageHttpClient = tenantRoutingHttpClientInitializer.builder(
                tenantRoutingProvider.getConnections("miru-manage", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build(); // TODO expose to conf

            ExecutorService tasExecutors = BoundedExecutor.newBoundedExecutor(1024, "tas");

            MiruClusterClient clusterClient = new MiruClusterClientInitializer(tasExecutors, 100, 95, 1000).initialize(miruStats, "", manageHttpClient, mapper);
            SickThreads walClientSickThreads = new SickThreads();

            /*
            Class<?> walEndpointsClass = null;

            MiruWALDirector<?, ?> secondaryWALDirector = null;
            MiruWALClient<?, ?> secondaryWALClient = null;
            Class<?> secondaryWALEndpointsClass = null;*/

            MiruWALClient<?, ?> miruWALClient = null;
            MiruWALDirector miruWALDirector = null;
            RCVSWALDirector rcvsWALDirector = null;
            AmzaWALDirector amzaWALDirector = null;
            MiruActivityWALReader<?, ?> activityWALReader = null;

            boolean primaryRCVSWAL = walConfig.getActivityWALType().equals("rcvs");
            boolean primaryAmzaWAL = walConfig.getActivityWALType().equals("amza");
            boolean secondaryAmzaWAL = walConfig.getSecondaryAmzaWAL();

            if (primaryRCVSWAL) {
                MiruWALClient<RCVSCursor, RCVSSipCursor> rcvsWALClient = new MiruWALClientInitializer().initialize("", walHttpClient,
                    tasExecutors,
                    100,
                    95,
                    1000,
                    mapper,
                    walClientSickThreads, 10_000,
                    "/miru/wal/rcvs", RCVSCursor.class, RCVSSipCursor.class);

                RCVSWALInitializer.RCVSWAL rcvsWAL = new RCVSWALInitializer().initialize(instanceConfig.getClusterName(),
                    rowColumnValueStoreInitializer,
                    mapper);

                Set<TenantAndPartition> blacklist = null;
                String tenantPartitionBlacklist = StringUtils.trimToNull(walConfig.getTenantPartitionBlacklist());
                if (tenantPartitionBlacklist != null) {
                    Builder<TenantAndPartition> builder = ImmutableSet.builder();
                    String[] tenantPartitions = tenantPartitionBlacklist.split("\\s*,\\s*");
                    for (int i = 0; i < tenantPartitions.length; i++) {
                        String[] split = tenantPartitions[i].split(":");
                        MiruTenantId tenantId = new MiruTenantId(split[0].getBytes(StandardCharsets.UTF_8));
                        MiruPartitionId partitionId = MiruPartitionId.of(Integer.parseInt(split[1]));
                        TenantAndPartition tenantAndPartition = new TenantAndPartition(tenantId, partitionId);
                        builder.add(tenantAndPartition);
                        LOG.info("Added {} to RCVS blacklist", tenantAndPartition);
                    }
                    blacklist = builder.build();
                }

                RCVSActivityWALWriter rcvsActivityWALWriter = new RCVSActivityWALWriter(rcvsWAL.getActivityWAL(), rcvsWAL.getActivitySipWAL(), blacklist);
                RCVSActivityWALReader rcvsActivityWALReader = new RCVSActivityWALReader(hostPortProvider,
                    rcvsWAL.getActivityWAL(),
                    rcvsWAL.getActivitySipWAL(),
                    blacklist);
                RCVSWALLookup rcvsWALLookup = new RCVSWALLookup(rcvsWAL.getWALLookupTable());

                RCVSReadTrackingWALWriter readTrackingWALWriter = new RCVSReadTrackingWALWriter(rcvsWAL.getReadTrackingWAL(), rcvsWAL.getReadTrackingSipWAL());
                RCVSReadTrackingWALReader readTrackingWALReader = new RCVSReadTrackingWALReader(hostPortProvider,
                    rcvsWAL.getReadTrackingWAL(),
                    rcvsWAL.getReadTrackingSipWAL());

                rcvsWALDirector = new RCVSWALDirector(rcvsWALLookup,
                    rcvsActivityWALReader,
                    rcvsActivityWALWriter,
                    readTrackingWALReader,
                    readTrackingWALWriter,
                    clusterClient);

                activityWALReader = rcvsActivityWALReader;
                miruWALClient = rcvsWALClient;
                miruWALDirector = new MiruWALDirector(rcvsWALLookup, rcvsWALClient, rcvsActivityWALReader, rcvsActivityWALWriter, clusterClient);

                deployable.addEndpoints(RCVSWALEndpoints.class);
                deployable.addInjectables(RCVSWALDirector.class, rcvsWALDirector);
            }

            if (primaryAmzaWAL || secondaryAmzaWAL) {
                MiruWALClient<AmzaCursor, AmzaSipCursor> amzaWALClient = new MiruWALClientInitializer().initialize("", walHttpClient,
                    tasExecutors,
                    100,
                    95,
                    1000,
                    mapper,
                    walClientSickThreads, 10_000,
                    "/miru/wal/amza", AmzaCursor.class, AmzaSipCursor.class);

                AmzaActivityWALWriter amzaActivityWALWriter = new AmzaActivityWALWriter(amzaWALUtil,
                    amzaServiceConfig.getReplicateTimeoutMillis(),
                    mapper);
                AmzaActivityWALReader amzaActivityWALReader = new AmzaActivityWALReader(amzaWALUtil, mapper);
                AmzaWALLookup amzaWALLookup = new AmzaWALLookup(amzaWALUtil,
                    amzaServiceConfig.getReplicateTimeoutMillis());

                AmzaReadTrackingWALWriter readTrackingWALWriter = new AmzaReadTrackingWALWriter(amzaWALUtil,
                    amzaServiceConfig.getReplicateTimeoutMillis(),
                    mapper);
                AmzaReadTrackingWALReader readTrackingWALReader = new AmzaReadTrackingWALReader(amzaWALUtil, mapper);

                amzaWALDirector = new AmzaWALDirector(amzaWALLookup,
                    amzaActivityWALReader,
                    amzaActivityWALWriter,
                    readTrackingWALReader,
                    readTrackingWALWriter,
                    clusterClient);

                deployable.addEndpoints(AmzaWALEndpoints.class);
                deployable.addInjectables(AmzaWALDirector.class, amzaWALDirector);

                if (primaryAmzaWAL) {
                    activityWALReader = amzaActivityWALReader;
                    miruWALClient = amzaWALClient;
                    miruWALDirector = new MiruWALDirector(amzaWALLookup, amzaWALClient, amzaActivityWALReader, amzaActivityWALWriter, clusterClient);
                }
            }

            if (walConfig.getActivityWALType().equals("rcvs_amza") || walConfig.getActivityWALType().equals("amza_rcvs")) {
                throw new IllegalStateException("Activity WAL type is no longer supported: " + walConfig.getActivityWALType());
            } else if (miruWALClient == null) {
                throw new IllegalStateException("Invalid activity WAL type: " + walConfig.getActivityWALType());
            }

            MiruWALRepair miruWALRepair = new MiruWALRepair(miruWALClient, activityWALReader);

            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);

            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                //.addResourcePath("../../../../../src/main/resources") // fluff?
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setDirectoryListingAllowed(false)
                .setContext("/ui/static");

            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);

            MiruWALUIService miruWALUIService = new MiruWALUIServiceInitializer()
                .initialize(instanceConfig.getClusterName(),
                    instanceConfig.getInstanceName(),
                    renderer,
                    amzaWALUtil,
                    tenantRoutingProvider,
                    miruWALClient,
                    miruWALDirector,
                    rcvsWALDirector,
                    amzaWALDirector,
                    activityWALReader,
                    miruStats);

            if (instanceConfig.getMainServiceAuthEnabled()) {
                deployable.addRouteOAuth("/miru/*");
                deployable.addSessionAuth("/ui/*", "/miru/*");
            } else {
                deployable.addNoAuth("/miru/*");
                deployable.addSessionAuth("/ui/*");
            }

            deployable.addEndpoints(MiruWALEndpoints.class);
            deployable.addInjectables(MiruWALUIService.class, miruWALUIService);
            deployable.addInjectables(MiruWALDirector.class, miruWALDirector);
            deployable.addInjectables(MiruWALClient.class, miruWALClient);
            deployable.addInjectables(MiruWALRepair.class, miruWALRepair);
            deployable.addInjectables(MiruStats.class, miruStats);

            deployable.addResource(sourceTree);
            deployable.addEndpoints(LoadBalancerHealthCheckEndpoints.class);
            deployable.buildServer().start();
            clientHealthProvider.start();
            isAmzaReady.set(amzaLifecycle::isReady);
            serviceStartupHealthCheck.success();

        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
