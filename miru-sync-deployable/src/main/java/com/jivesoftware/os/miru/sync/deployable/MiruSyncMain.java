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
package com.jivesoftware.os.miru.sync.deployable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.api.partition.Consistency;
import com.jivesoftware.os.amza.api.partition.Durability;
import com.jivesoftware.os.amza.api.partition.PartitionProperties;
import com.jivesoftware.os.amza.api.stream.RowType;
import com.jivesoftware.os.amza.client.aquarium.AmzaClientAquariumProvider;
import com.jivesoftware.os.amza.client.collection.AmzaMarshaller;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.client.http.HttpPartitionClientFactory;
import com.jivesoftware.os.amza.client.http.HttpPartitionHostsProvider;
import com.jivesoftware.os.amza.client.http.RingHostHttpClientProvider;
import com.jivesoftware.os.aquarium.AquariumStats;
import com.jivesoftware.os.aquarium.Member;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.JiveEpochTimestampProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.jive.utils.ordered.id.SnowflakeIdPacker;
import com.jivesoftware.os.jive.utils.ordered.id.TimestampedOrderIdProvider;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.sync.ActivityReadEventConverter;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.wal.AmzaCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.MiruWALConfig;
import com.jivesoftware.os.miru.api.wal.RCVSCursor;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.cluster.client.ClusterSchemaProvider;
import com.jivesoftware.os.miru.cluster.client.MiruClusterClientInitializer;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer.MiruLogAppenderConfig;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer.MiruMetricSamplerConfig;
import com.jivesoftware.os.miru.sync.api.MiruSyncSenderConfig;
import com.jivesoftware.os.miru.sync.api.MiruSyncTenantConfig;
import com.jivesoftware.os.miru.sync.api.MiruSyncTenantTuple;
import com.jivesoftware.os.miru.sync.api.MiruSyncTimeShiftStrategy;
import com.jivesoftware.os.miru.sync.deployable.endpoints.MiruSyncApiEndpoints;
import com.jivesoftware.os.miru.sync.deployable.endpoints.MiruSyncEndpoints;
import com.jivesoftware.os.miru.sync.deployable.oauth.MiruSyncOAuthValidatorInitializer;
import com.jivesoftware.os.miru.sync.deployable.oauth.MiruSyncOAuthValidatorInitializer.MiruSyncOAuthValidatorConfig;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.wal.client.MiruWALClientInitializer;
import com.jivesoftware.os.miru.wal.client.MiruWALClientInitializer.WALClientSickThreadsHealthCheckConfig;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.deployable.DeployableHealthCheckRegistry;
import com.jivesoftware.os.routing.bird.deployable.ErrorHealthCheckConfig;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.endpoints.base.FullyOnlineVersion;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI.UI;
import com.jivesoftware.os.routing.bird.endpoints.base.LoadBalancerHealthCheckEndpoints;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.checkers.FileDescriptorCountHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCPauseHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.LoadAverageHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreads;
import com.jivesoftware.os.routing.bird.health.checkers.SickThreadsHealthCheck;
import com.jivesoftware.os.routing.bird.health.checkers.SystemCpuHealthChecker;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.HttpDeliveryClientHealthProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.http.client.TenantRoutingHttpClientInitializer;
import com.jivesoftware.os.routing.bird.server.oauth.validator.AuthValidator;
import com.jivesoftware.os.routing.bird.server.util.Resource;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptor;
import com.jivesoftware.os.routing.bird.shared.ConnectionDescriptors;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;
import com.jivesoftware.os.routing.bird.shared.TenantsServiceConnectionDescriptorProvider;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.glassfish.jersey.oauth1.signature.OAuth1Request;
import org.glassfish.jersey.oauth1.signature.OAuth1Signature;

public class MiruSyncMain {

    public static void main(String[] args) throws Exception {
        new MiruSyncMain().run(args);
    }

    void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            final Deployable deployable = new Deployable(args);
            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);
            HealthFactory.initialize(deployable::config, new DeployableHealthCheckRegistry(deployable));
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(new UI("Miru-Sync", "main", "/ui"))));

            deployable.addHealthCheck(new GCPauseHealthChecker(deployable.config(GCPauseHealthChecker.GCPauseHealthCheckerConfig.class)));
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(new SystemCpuHealthChecker(deployable.config(SystemCpuHealthChecker.SystemCpuHealthCheckerConfig.class)));
            deployable.addHealthCheck(new LoadAverageHealthChecker(deployable.config(LoadAverageHealthChecker.LoadAverageHealthCheckerConfig.class)));
            deployable.addHealthCheck(
                new FileDescriptorCountHealthChecker(deployable.config(FileDescriptorCountHealthChecker.FileDescriptorCountHealthCheckerConfig.class)));
            deployable.addHealthCheck(serviceStartupHealthCheck);
            deployable.addErrorHealthChecks(deployable.config(ErrorHealthCheckConfig.class));
            deployable.addManageInjectables(FullyOnlineVersion.class, (FullyOnlineVersion) () -> {
                if (serviceStartupHealthCheck.startupHasSucceeded()) {
                    return instanceConfig.getVersion();
                } else {
                    return null;
                }
            });
            deployable.buildManageServer().start();


            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            mapper.registerModule(new GuavaModule());

            TenantRoutingProvider tenantRoutingProvider = deployable.getTenantRoutingProvider();
            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = deployable.getTenantRoutingHttpClientInitializer();
            HttpDeliveryClientHealthProvider clientHealthProvider = new HttpDeliveryClientHealthProvider(instanceConfig.getInstanceKey(),
                HttpRequestHelperUtils.buildRequestHelper(false, false, null, instanceConfig.getRoutesHost(), instanceConfig.getRoutesPort()),
                instanceConfig.getConnectionsHealth(), 5_000, 100);

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

            MiruSyncConfig syncConfig = deployable.config(MiruSyncConfig.class);

            TenantsServiceConnectionDescriptorProvider syncDescriptorProvider = tenantRoutingProvider
                .getConnections(instanceConfig.getServiceName(), "main", 10_000); // TODO config

            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> walHttpClient = tenantRoutingHttpClientInitializer.builder(
                tenantRoutingProvider.getConnections("miru-wal", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build(); // TODO expose to conf

            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> writerHttpClient = tenantRoutingHttpClientInitializer.builder(
                tenantRoutingProvider.getConnections("miru-writer", "main", 10_000), // TODO config
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

            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> amzaClient = tenantRoutingHttpClientInitializer.builder(
                tenantRoutingProvider.getConnections("amza", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .socketTimeoutInMillis(10_000)
                .build(); // TODO expose to conf

            BAInterner interner = new BAInterner();
            AmzaClientProvider<HttpClient, HttpClientException> amzaClientProvider = new AmzaClientProvider<>(
                new HttpPartitionClientFactory(interner),
                new HttpPartitionHostsProvider(interner, amzaClient, mapper),
                new RingHostHttpClientProvider(amzaClient),
                Executors.newFixedThreadPool(syncConfig.getAmzaCallerThreadPoolSize()),
                syncConfig.getAmzaAwaitLeaderElectionForNMillis(),
                -1,
                -1);

            TimestampedOrderIdProvider orderIdProvider = new OrderIdProviderImpl(
                new ConstantWriterIdProvider(instanceConfig.getInstanceName()),
                new SnowflakeIdPacker(),
                new JiveEpochTimestampProvider());
            AmzaClientAquariumProvider amzaClientAquariumProvider = new AmzaClientAquariumProvider(new AquariumStats(),
                instanceConfig.getServiceName(),
                amzaClientProvider,
                orderIdProvider,
                new Member(instanceConfig.getInstanceKey().getBytes(StandardCharsets.UTF_8)),
                count -> {
                    ConnectionDescriptors descriptors = syncDescriptorProvider.getConnections("");
                    int ringSize = descriptors.getConnectionDescriptors().size();
                    return count > ringSize / 2;
                },
                () -> {
                    Set<Member> members = Sets.newHashSet();
                    ConnectionDescriptors descriptors = syncDescriptorProvider.getConnections("");
                    for (ConnectionDescriptor connectionDescriptor : descriptors.getConnectionDescriptors()) {
                        members.add(new Member(connectionDescriptor.getInstanceDescriptor().instanceKey.getBytes(StandardCharsets.UTF_8)));
                    }
                    return members;
                },
                128, //TODO config
                128, //TODO config
                5_000L, //TODO config
                100L, //TODO config
                60_000L, //TODO config
                10_000L, //TODO config
                Executors.newSingleThreadExecutor(),
                100L, //TODO config
                1_000L, //TODO config
                10_000L, //TODO config
                syncConfig.getUseClientSolutionLog());

            AmzaClientProvider clientProvider = new AmzaClientProvider<>(
                new HttpPartitionClientFactory(interner),
                new HttpPartitionHostsProvider(interner, amzaClient, mapper),
                new RingHostHttpClientProvider(amzaClient),
                Executors.newCachedThreadPool(), //TODO expose to conf?
                30_000L, // TODO config
                -1,
                -1);

            MiruSyncConfigStorage miruSyncConfigStorage = new MiruSyncConfigStorage(clientProvider,
                "miru-sync-config-",
                new PartitionProperties(Durability.fsync_async,
                    0, 0, 0, 0, 0, 0, 0, 0,
                    false,
                    Consistency.leader_quorum,
                    true,
                    true,
                    false,
                    RowType.snappy_primary,
                    "lab",
                    -1,
                    null,
                    -1,
                    -1),
                new AmzaMarshaller<MiruSyncTenantTuple>() {
                    @Override
                    public MiruSyncTenantTuple fromBytes(byte[] bytes) throws Exception {
                        return MiruSyncTenantTuple.fromBytes(bytes);
                    }

                    @Override
                    public byte[] toBytes(MiruSyncTenantTuple miruSyncTenantTuple) throws Exception {
                        return MiruSyncTenantTuple.toBytes(miruSyncTenantTuple);
                    }
                },
                new AmzaMarshaller<MiruSyncTenantConfig>() {
                    @Override
                    public MiruSyncTenantConfig fromBytes(byte[] bytes) throws Exception {
                        return mapper.readValue(bytes, MiruSyncTenantConfig.class);
                    }

                    @Override
                    public byte[] toBytes(MiruSyncTenantConfig miruSyncTenantConfig) throws Exception {
                        return mapper.writeValueAsBytes(miruSyncTenantConfig);
                    }
                }
            );


            MiruSyncSenderConfigStorage miruSyncSenderConfigStorage = new MiruSyncSenderConfigStorage(clientProvider,
                "miru-sync-sender-config",
                new PartitionProperties(Durability.fsync_async,
                    0, 0, 0, 0, 0, 0, 0, 0,
                    false,
                    Consistency.leader_quorum,
                    true,
                    true,
                    false,
                    RowType.snappy_primary,
                    "lab",
                    -1,
                    null,
                    -1,
                    -1),
                new AmzaMarshaller<String>() {
                    @Override
                    public String fromBytes(byte[] bytes) throws Exception {
                        return new String(bytes, StandardCharsets.UTF_8);
                    }

                    @Override
                    public byte[] toBytes(String s) throws Exception {
                        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
                    }
                },
                new AmzaMarshaller<MiruSyncSenderConfig>() {
                    @Override
                    public MiruSyncSenderConfig fromBytes(byte[] bytes) throws Exception {
                        return mapper.readValue(bytes, MiruSyncSenderConfig.class);
                    }

                    @Override
                    public byte[] toBytes(MiruSyncSenderConfig miruSyncSenderConfig) throws Exception {
                        return mapper.writeValueAsBytes(miruSyncSenderConfig);
                    }
                }
            );


            SickThreads walClientSickThreads = new SickThreads();
            deployable.addHealthCheck(new SickThreadsHealthCheck(deployable.config(WALClientSickThreadsHealthCheckConfig.class), walClientSickThreads));

            MiruStats miruStats = new MiruStats();
            MiruClusterClient clusterClient = new MiruClusterClientInitializer().initialize(miruStats, "", manageHttpClient, mapper);

            ActivityReadEventConverter activityReadEventConverter = syncConfig.getSyncReceiverActivityReadEventConverterClass().newInstance();

            MiruWALConfig walConfig = deployable.config(MiruWALConfig.class);
            MiruSyncReceiver<?, ?> syncReceiver = null;
            MiruSyncCopier<?, ?> syncCopier;
            MiruSyncSenders<?, ?> syncSenders = null;

            boolean syncLoopback = false;
            LoopbackSyncClient loopbackSyncClient = null;
            if (syncConfig.getSyncLoopback().equals("rcvs")) {
                MiruWALClient<RCVSCursor, RCVSSipCursor> rcvsWALClient = new MiruWALClientInitializer().initialize("",
                    walHttpClient,
                    mapper,
                    walClientSickThreads,
                    10_000,
                    "/miru/wal/rcvs",
                    RCVSCursor.class,
                    RCVSSipCursor.class);
                loopbackSyncClient = new LoopbackSyncClient(rcvsWALClient);
                syncLoopback = true;
            } else if (syncConfig.getSyncLoopback().equals("amza")) {
                MiruWALClient<AmzaCursor, AmzaSipCursor> amzaWALClient = new MiruWALClientInitializer().initialize("",
                    walHttpClient,
                    mapper,
                    walClientSickThreads, 10_000,
                    "/miru/wal/amza",
                    AmzaCursor.class,
                    AmzaSipCursor.class);
                loopbackSyncClient = new LoopbackSyncClient(amzaWALClient);
                syncLoopback = true;
            }

            if (walConfig.getActivityWALType().equals("rcvs")) {
                MiruWALClient<RCVSCursor, RCVSSipCursor> rcvsWALClient = new MiruWALClientInitializer().initialize("",
                    walHttpClient,
                    mapper,
                    walClientSickThreads,
                    10_000,
                    "/miru/wal/rcvs",
                    RCVSCursor.class,
                    RCVSSipCursor.class);
                syncCopier = (MiruSyncCopier) new MiruSyncCopier<>(rcvsWALClient, syncConfig.getCopyBatchSize(), RCVSCursor.INITIAL, RCVSCursor.class);

                if (syncConfig.getSyncReceiverEnabled()) {
                    syncReceiver = (MiruSyncReceiver) new MiruSyncReceiver<>(rcvsWALClient, writerHttpClient, clusterClient, activityReadEventConverter);
                }

                if (syncConfig.getSyncSenderEnabled()) {
                    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(syncConfig.getSyncSendersThreadCount());
                    ClusterSchemaProvider schemaProvider = new ClusterSchemaProvider(clusterClient, 10_000);

                    MiruSyncConfigProvider loopbackSyncConfigProvider = null;
                    if (syncLoopback) {
                        long loopbackSyncDurationMillis = syncConfig.getSyncLoopbackDurationMillis();
                        loopbackSyncConfigProvider = senderName -> {
                            List<MiruTenantId> tenantIds = rcvsWALClient.getAllTenantIds();
                            Map<MiruSyncTenantTuple, MiruSyncTenantConfig> tenantTupleConfigs = Maps.newHashMap();
                            MiruSyncTenantConfig config = new MiruSyncTenantConfig(System.currentTimeMillis() - loopbackSyncDurationMillis,
                                Long.MAX_VALUE,
                                0,
                                MiruSyncTimeShiftStrategy.none);
                            for (MiruTenantId tenantId : tenantIds) {
                                tenantTupleConfigs.put(new MiruSyncTenantTuple(tenantId, tenantId), config);
                            }
                            return tenantTupleConfigs;
                        };
                    }
                    //  don't remove generics (fails compilation for others when we do)
                    syncSenders = (MiruSyncSenders) new MiruSyncSenders<>(
                        miruStats,
                        syncConfig,
                        orderIdProvider,
                        executorService,
                        amzaClientProvider,
                        amzaClientAquariumProvider,
                        mapper,
                        schemaProvider,
                        miruSyncSenderConfigStorage,
                        miruSyncConfigStorage,
                        30_000, // TODO config
                        rcvsWALClient,
                        syncLoopback,
                        loopbackSyncClient,
                        loopbackSyncConfigProvider,
                        RCVSCursor.INITIAL,
                        RCVSCursor.class);
                }

            } else if (walConfig.getActivityWALType().equals("amza")) {
                MiruWALClient<AmzaCursor, AmzaSipCursor> amzaWALClient = new MiruWALClientInitializer().initialize("",
                    walHttpClient,
                    mapper,
                    walClientSickThreads, 10_000,
                    "/miru/wal/amza",
                    AmzaCursor.class,
                    AmzaSipCursor.class);

                syncCopier = (MiruSyncCopier) new MiruSyncCopier<>(amzaWALClient, syncConfig.getCopyBatchSize(), null, AmzaCursor.class);

                if (syncConfig.getSyncReceiverEnabled()) {
                    syncReceiver = (MiruSyncReceiver) new MiruSyncReceiver<>(amzaWALClient, writerHttpClient, clusterClient, activityReadEventConverter);
                }

                if (syncConfig.getSyncSenderEnabled()) {
                    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(syncConfig.getSyncSendersThreadCount());
                    ClusterSchemaProvider schemaProvider = new ClusterSchemaProvider(clusterClient, 10_000);

                    MiruSyncConfigProvider loopbackSyncConfigProvider = null;
                    if (syncLoopback) {
                        long loopbackSyncDurationMillis = syncConfig.getSyncLoopbackDurationMillis();
                        loopbackSyncConfigProvider = senderName -> {
                            List<MiruTenantId> tenantIds = amzaWALClient.getAllTenantIds();
                            Map<MiruSyncTenantTuple, MiruSyncTenantConfig> tenantTupleConfigs = Maps.newHashMap();
                            MiruSyncTenantConfig config = new MiruSyncTenantConfig(System.currentTimeMillis() - loopbackSyncDurationMillis,
                                Long.MAX_VALUE,
                                0,
                                MiruSyncTimeShiftStrategy.none);
                            for (MiruTenantId tenantId : tenantIds) {
                                tenantTupleConfigs.put(new MiruSyncTenantTuple(tenantId, tenantId), config);
                            }
                            return tenantTupleConfigs;
                        };
                    }
                    //  don't remove generics (fails compilation for others when we do)
                    syncSenders = (MiruSyncSenders) new MiruSyncSenders<>(
                        miruStats,
                        syncConfig,
                        orderIdProvider,
                        executorService,
                        amzaClientProvider,
                        amzaClientAquariumProvider,
                        mapper,
                        schemaProvider,
                        miruSyncSenderConfigStorage,
                        miruSyncConfigStorage,
                        30_000, // TODO config
                        amzaWALClient,
                        syncLoopback,
                        loopbackSyncClient,
                        loopbackSyncConfigProvider,
                        null,
                        AmzaCursor.class);
                }

            } else {
                throw new IllegalStateException("Invalid activity WAL type: " + walConfig.getActivityWALType());
            }


            amzaClientAquariumProvider.start();
            if (syncSenders != null) {
                syncSenders.start();
            }

            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);

            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                //.addResourcePath("../../../../../src/main/resources") // fluff?
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setDirectoryListingAllowed(false)
                .setContext("/ui/static");

            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);

            MiruSyncUIService miruSyncUIService = new MiruSyncUIServiceInitializer().initialize(instanceConfig.getClusterName(),
                instanceConfig.getInstanceName(),
                renderer,
                syncSenders,
                miruStats,
                tenantRoutingProvider,
                mapper);

            deployable.addNoAuth("/health/check");
            if (instanceConfig.getMainServiceAuthEnabled()) {
                if (syncConfig.getSyncReceiverEnabled()) {
                    MiruSyncOAuthValidatorConfig oAuthValidatorConfig = deployable.config(MiruSyncOAuthValidatorConfig.class);
                    AuthValidator<OAuth1Signature, OAuth1Request> syncOAuthValidator = new MiruSyncOAuthValidatorInitializer()
                        .initialize(oAuthValidatorConfig);
                    deployable.addCustomOAuth(syncOAuthValidator, "/api/*");
                }
                deployable.addRouteOAuth("/miru/*", "/api/*");
                deployable.addSessionAuth("/ui/*", "/miru/*", "/api/*");
            } else {
                deployable.addNoAuth("/miru/*", "/api/*");
                deployable.addSessionAuth("/ui/*");
            }

            deployable.addEndpoints(MiruSyncEndpoints.class);
            deployable.addInjectables(MiruSyncCopier.class, syncCopier);
            if (syncSenders != null) {
                deployable.addInjectables(MiruSyncSenders.class, syncSenders);
            }

            deployable.addEndpoints(MiruSyncUIEndpoints.class);
            deployable.addInjectables(MiruSyncUIService.class, miruSyncUIService);
            deployable.addInjectables(MiruStats.class, miruStats);

            if (syncConfig.getSyncReceiverEnabled()) {
                deployable.addEndpoints(MiruSyncApiEndpoints.class);
                deployable.addInjectables(MiruSyncReceiver.class, syncReceiver);
            }

            deployable.addInjectables(ObjectMapper.class, mapper);
            deployable.addInjectables(MiruSyncConfigStorage.class, miruSyncConfigStorage);
            deployable.addInjectables(MiruSyncSenderConfigStorage.class, miruSyncSenderConfigStorage);


            deployable.addResource(sourceTree);
            deployable.addEndpoints(LoadBalancerHealthCheckEndpoints.class);
            deployable.buildServer().start();
            clientHealthProvider.start();
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
