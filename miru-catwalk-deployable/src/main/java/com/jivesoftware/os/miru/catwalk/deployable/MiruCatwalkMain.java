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
package com.jivesoftware.os.miru.catwalk.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.client.http.HttpPartitionClientFactory;
import com.jivesoftware.os.amza.client.http.HttpPartitionHostsProvider;
import com.jivesoftware.os.amza.client.http.RingHostHttpClientProvider;
import com.jivesoftware.os.amza.embed.EmbedAmzaServiceInitializer.Lifecycle;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceConfig;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceInitializer;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.catwalk.deployable.endpoints.CatwalkModelEndpoints;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer.MiruLogAppenderConfig;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer.MiruMetricSamplerConfig;
import com.jivesoftware.os.miru.plugin.query.MiruTenantQueryRouting;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.wal.client.MiruWALClientInitializer.WALClientSickThreadsHealthCheckConfig;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.deployable.DeployableHealthCheckRegistry;
import com.jivesoftware.os.routing.bird.deployable.ErrorHealthCheckConfig;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.deployable.TenantAwareHttpClientHealthCheck;
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
import com.jivesoftware.os.routing.bird.http.client.HttpDeliveryClientHealthProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.TailAtScaleStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.http.client.TenantRoutingHttpClientInitializer;
import com.jivesoftware.os.routing.bird.server.util.Resource;
import com.jivesoftware.os.routing.bird.shared.HttpClientException;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;
import java.io.File;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.merlin.config.Config;
import org.merlin.config.defaults.BooleanDefault;
import org.merlin.config.defaults.FloatDefault;
import org.merlin.config.defaults.IntDefault;
import org.merlin.config.defaults.LongDefault;
import org.merlin.config.defaults.StringDefault;

public class MiruCatwalkMain {

    public static void main(String[] args) throws Exception {
        new MiruCatwalkMain().run(args);
    }

    public interface AmzaCatwalkConfig extends MiruAmzaServiceConfig {

        @StringDefault("./var/amza/catwalk/data/")
        @Override
        String getWorkingDirectories();

        @IntDefault(64)
        int getAmzaCallerThreadPoolSize();

        @LongDefault(10_000)
        long getAmzaAwaitLeaderElectionForNMillis();

        @IntDefault(64)
        int getNumberOfUpateModelQueues();

        @IntDefault(1_000)
        int getCheckQueuesBatchSize();

        @LongDefault(10_000)
        long getCheckQueuesForWorkEvenNMillis();

        @LongDefault(1_000L * 60 * 60)
        long getModelUpdateIntervalInMillis();

        @LongDefault(60_000)
        long getQueueFailureDelayInMillis();

        @FloatDefault(0.0001f)
        float getUpdateMinFeatureScore();

        @FloatDefault(0.001f)
        float getRepairMinFeatureScore();

        @FloatDefault(0.01f)
        float getGatherMinFeatureScore();

        @IntDefault(10_000)
        int getUpdateMaxFeatureScoresPerFeature();

        @IntDefault(1_000)
        int getRepairMaxFeatureScoresPerFeature();

        @IntDefault(100)
        int getGatherMaxFeatureScoresPerFeature();

        @BooleanDefault(false)
        boolean getUseScanCompression();

        @LongDefault(1_000)
        long getAdditionalSolverAfterNMillis();

        @LongDefault(10_000)
        long getAbandonLeaderSolutionAfterNMillis();

        @LongDefault(30_000)
        long getAbandonSolutionAfterNMillis();

        @LongDefault(-1)
        long getAmzaDebugClientCount();

        @LongDefault(-1)
        long getAmzaDebugClientCountInterval();

        @IntDefault(4)
        int getModelUpdaterThreadPoolSize();

        @IntDefault(4)
        int getReadRepairThreadPoolSize();
    }

    public interface MiruCatwalkConfig extends Config {
        @BooleanDefault(false)
        boolean getTailAtScaleEnabled();

        @IntDefault(64)
        int getTailAtScaleThreadCount();

        @IntDefault(1_000)
        int getTailAtScaleWindowSize();

        @FloatDefault(95f)
        float getTailAtScalePercentile();
    }

    void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            final Deployable deployable = new Deployable(args);
            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class); //config(DevInstanceConfig.class);

            HealthFactory.initialize(deployable::config, new DeployableHealthCheckRegistry(deployable));
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(
                new UI("Miru-Catwalk", "main", "/ui"),
                new UI("Miru-Catwalk-Amza", "main", "/amza/ui"))));
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


            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            mapper.registerModule(new GuavaModule());

            HttpResponseMapper responseMapper = new HttpResponseMapper(mapper);

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

            deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("stump", miruStumptownClient));

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

            deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("anomaly", miruAnomalyClient));

            new MiruMetricSamplerInitializer().initialize(
                instanceConfig.getDatacenter(),
                instanceConfig.getClusterName(),
                instanceConfig.getHost(),
                instanceConfig.getServiceName(),
                String.valueOf(instanceConfig.getInstanceName()),
                instanceConfig.getVersion(),
                metricSamplerConfig,
                miruAnomalyClient).start();

            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);

            AmzaCatwalkConfig amzaCatwalkConfig = deployable.config(AmzaCatwalkConfig.class);
            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> amzaClient = tenantRoutingHttpClientInitializer.builder(
                tenantRoutingProvider.getConnections("amza", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .debugClient(amzaCatwalkConfig.getAmzaDebugClientCount(), amzaCatwalkConfig.getAmzaDebugClientCountInterval())
                .build(); // TODO expose to conf

            deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("amza", amzaClient));

            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> manageHttpClient = tenantRoutingHttpClientInitializer.builder(
                tenantRoutingProvider.getConnections("miru-manage", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build(); // TODO expose to conf

            deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("manage", manageHttpClient));

            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> readerClient = tenantRoutingHttpClientInitializer.builder(
                tenantRoutingProvider.getConnections("miru-reader", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build(); // TODO expose to conf

            deployable.addHealthCheck(new TenantAwareHttpClientHealthCheck("reader", readerClient));

            SickThreads walClientSickThreads = new SickThreads();
            deployable.addHealthCheck(new SickThreadsHealthCheck(deployable.config(WALClientSickThreadsHealthCheckConfig.class), walClientSickThreads));

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
                null, //"amza-topology-" + instanceConfig.getClusterName(), // Manual service discovery if null
                amzaCatwalkConfig,
                true,
                -1,
                rowsChanged -> {
                });

            TailAtScaleStrategy tailAtScaleStrategy = new TailAtScaleStrategy(
                deployable.newBoundedExecutor(1024, "amza-client-tas"),
                100, // TODO config
                95, // TODO config
                1000
            );

            AmzaClientProvider<HttpClient, HttpClientException> amzaClientProvider = new AmzaClientProvider<>(
                new HttpPartitionClientFactory(),
                new HttpPartitionHostsProvider(amzaClient, tailAtScaleStrategy, mapper),
                new RingHostHttpClientProvider(amzaClient),
                deployable.newBoundedExecutor(amzaCatwalkConfig.getAmzaCallerThreadPoolSize(), "amza-client"),
                amzaCatwalkConfig.getAmzaAwaitLeaderElectionForNMillis(),
                amzaCatwalkConfig.getAmzaDebugClientCount(),
                amzaCatwalkConfig.getAmzaDebugClientCountInterval());

            EmbeddedClientProvider embeddedClientProvider = new EmbeddedClientProvider(amzaLifecycle.amzaService);

            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);

            MiruStats stats = new MiruStats();

            int numProcs = Runtime.getRuntime().availableProcessors();
            ScheduledExecutorService queueConsumers = Executors.newScheduledThreadPool(numProcs, new ThreadFactoryBuilder().setNameFormat("queueConsumers-%d")
                .build());

            ExecutorService modelUpdaters = deployable.newBoundedExecutor(amzaCatwalkConfig.getModelUpdaterThreadPoolSize(), "model-updater");
            ExecutorService readRepairers = deployable.newBoundedExecutor(amzaCatwalkConfig.getReadRepairThreadPoolSize(), "read-repair");
            ExecutorService tasExecutors = deployable.newBoundedExecutor(1024, "reader-tas");

            MiruCatwalkConfig catwalkConfig = deployable.config(MiruCatwalkConfig.class);

            MiruTenantQueryRouting tenantQueryRouting = new MiruTenantQueryRouting(readerClient,
                mapper,
                responseMapper,
                tasExecutors,
                catwalkConfig.getTailAtScaleWindowSize(),
                catwalkConfig.getTailAtScalePercentile(),
                1000,
                catwalkConfig.getTailAtScaleEnabled());

            CatwalkModelQueue catwalkModelQueue = new CatwalkModelQueue(amzaLifecycle.amzaService,
                embeddedClientProvider,
                mapper,
                amzaCatwalkConfig.getNumberOfUpateModelQueues());
            CatwalkModelService catwalkModelService = new CatwalkModelService(catwalkModelQueue,
                readRepairers,
                amzaClientProvider,
                stats,
                amzaCatwalkConfig.getRepairMinFeatureScore(),
                amzaCatwalkConfig.getGatherMinFeatureScore(),
                amzaCatwalkConfig.getRepairMaxFeatureScoresPerFeature(),
                amzaCatwalkConfig.getGatherMaxFeatureScoresPerFeature(),
                amzaCatwalkConfig.getUseScanCompression(),
                amzaCatwalkConfig.getAdditionalSolverAfterNMillis(),
                amzaCatwalkConfig.getAbandonLeaderSolutionAfterNMillis(),
                amzaCatwalkConfig.getAbandonSolutionAfterNMillis());

            CatwalkModelUpdater catwalkModelUpdater = new CatwalkModelUpdater(catwalkModelService,
                catwalkModelQueue,
                queueConsumers,
                tenantQueryRouting,
                modelUpdaters,
                amzaLifecycle.amzaService,
                embeddedClientProvider,
                stats,
                amzaCatwalkConfig.getModelUpdateIntervalInMillis(),
                amzaCatwalkConfig.getQueueFailureDelayInMillis(),
                amzaCatwalkConfig.getUpdateMinFeatureScore(),
                amzaCatwalkConfig.getUpdateMaxFeatureScoresPerFeature());

            MiruCatwalkUIService miruCatwalkUIService = new MiruCatwalkUIInitializer().initialize(
                instanceConfig.getClusterName(),
                instanceConfig.getInstanceName(),
                renderer,
                stats,
                tenantRoutingProvider,
                catwalkModelService);

            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setDirectoryListingAllowed(false)
                .setContext("/ui/static");

            if (instanceConfig.getMainServiceAuthEnabled()) {
                deployable.addRouteOAuth("/miru/*");
                deployable.addSessionAuth("/ui/*", "/miru/*");
            } else {
                deployable.addNoAuth("/miru/*");
                deployable.addSessionAuth("/ui/*");
            }

            deployable.addEndpoints(CatwalkModelEndpoints.class);
            deployable.addInjectables(CatwalkModelService.class, catwalkModelService);
            deployable.addInjectables(CatwalkModelUpdater.class, catwalkModelUpdater);
            deployable.addInjectables(ObjectMapper.class, mapper);

            deployable.addEndpoints(MiruCatwalkUIEndpoints.class);
            deployable.addInjectables(MiruCatwalkUIService.class, miruCatwalkUIService);
            deployable.addInjectables(MiruStats.class, stats);

            deployable.addResource(sourceTree);
            deployable.addEndpoints(LoadBalancerHealthCheckEndpoints.class);
            deployable.buildServer().start();
            clientHealthProvider.start();
            catwalkModelUpdater.start(amzaCatwalkConfig.getNumberOfUpateModelQueues(),
                amzaCatwalkConfig.getCheckQueuesBatchSize(),
                amzaCatwalkConfig.getCheckQueuesForWorkEvenNMillis());
            isAmzaReady.set(amzaLifecycle::isReady);
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
