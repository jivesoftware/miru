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
import com.jivesoftware.os.amza.api.BAInterner;
import com.jivesoftware.os.amza.client.http.AmzaClientProvider;
import com.jivesoftware.os.amza.client.http.HttpPartitionClientFactory;
import com.jivesoftware.os.amza.client.http.HttpPartitionHostsProvider;
import com.jivesoftware.os.amza.client.http.RingHostHttpClientProvider;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceConfig;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceInitializer;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.catwalk.deployable.endpoints.CatwalkModelEndpoints;
import com.jivesoftware.os.miru.logappender.MiruLogAppender;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.logappender.RoutingBirdLogSenderProvider;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSampler;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer.MiruMetricSamplerConfig;
import com.jivesoftware.os.miru.metric.sampler.RoutingBirdMetricSampleSenderProvider;
import com.jivesoftware.os.miru.plugin.query.MiruTenantQueryRouting;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.wal.client.MiruWALClientInitializer.WALClientSickThreadsHealthCheckConfig;
import com.jivesoftware.os.routing.bird.deployable.AuthValidationFilter;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.deployable.DeployableHealthCheckRegistry;
import com.jivesoftware.os.routing.bird.deployable.ErrorHealthCheckConfig;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI;
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
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.HttpDeliveryClientHealthProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;
import com.jivesoftware.os.routing.bird.http.client.HttpResponseMapper;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.http.client.TenantRoutingHttpClientInitializer;
import com.jivesoftware.os.routing.bird.server.util.Resource;
import com.jivesoftware.os.routing.bird.shared.TenantRoutingProvider;
import com.jivesoftware.os.routing.bird.shared.TenantsServiceConnectionDescriptorProvider;
import java.io.File;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
    }

    public void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            final Deployable deployable = new Deployable(args);
            HealthFactory.initialize(deployable::config, new DeployableHealthCheckRegistry(deployable));
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(
                new HasUI.UI("Reset Errors", "manage", "/manage/resetErrors"),
                new HasUI.UI("Reset Health", "manage", "/manage/resetHealth"),
                new HasUI.UI("Tail", "manage", "/manage/tail?lastNLines=1000"),
                new HasUI.UI("Thread Dump", "manage", "/manage/threadDump"),
                new HasUI.UI("Health", "manage", "/manage/ui"),
                new HasUI.UI("Miru-Catwalk", "main", "/ui"),
                new HasUI.UI("Miru-Catwalk-Amza", "main", "/amza"))));
            deployable.buildStatusReporter(null).start();
            deployable.addHealthCheck(new GCPauseHealthChecker(deployable.config(GCPauseHealthChecker.GCPauseHealthCheckerConfig.class)));
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(new SystemCpuHealthChecker(deployable.config(SystemCpuHealthChecker.SystemCpuHealthCheckerConfig.class)));
            deployable.addHealthCheck(new LoadAverageHealthChecker(deployable.config(LoadAverageHealthChecker.LoadAverageHealthCheckerConfig.class)));
            deployable.addHealthCheck(
                new FileDescriptorCountHealthChecker(deployable.config(FileDescriptorCountHealthChecker.FileDescriptorCountHealthCheckerConfig.class)));
            deployable.addHealthCheck(serviceStartupHealthCheck);
            deployable.addErrorHealthChecks(deployable.config(ErrorHealthCheckConfig.class));
            deployable.buildManageServer().start();

            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class); //config(DevInstanceConfig.class);

            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            mapper.registerModule(new GuavaModule());

            HttpResponseMapper responseMapper = new HttpResponseMapper(mapper);

            TenantRoutingProvider tenantRoutingProvider = deployable.getTenantRoutingProvider();
            MiruLogAppenderInitializer.MiruLogAppenderConfig miruLogAppenderConfig = deployable.config(MiruLogAppenderInitializer.MiruLogAppenderConfig.class);
            TenantsServiceConnectionDescriptorProvider logConnections = tenantRoutingProvider.getConnections("miru-stumptown", "main", 10_000); // TODO config
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
            TenantsServiceConnectionDescriptorProvider metricConnections = tenantRoutingProvider.getConnections("miru-anomaly", "main", 10_000); // TODO config
            MiruMetricSampler sampler = new MiruMetricSamplerInitializer().initialize(null, //TODO datacenter
                instanceConfig.getClusterName(),
                instanceConfig.getHost(),
                instanceConfig.getServiceName(),
                String.valueOf(instanceConfig.getInstanceName()),
                instanceConfig.getVersion(),
                metricSamplerConfig,
                new RoutingBirdMetricSampleSenderProvider<>(metricConnections, "", metricSamplerConfig.getSocketTimeoutInMillis()));
            sampler.start();

            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);

            HttpDeliveryClientHealthProvider clientHealthProvider = new HttpDeliveryClientHealthProvider(instanceConfig.getInstanceKey(),
                HttpRequestHelperUtils.buildRequestHelper(false, false, null, instanceConfig.getRoutesHost(), instanceConfig.getRoutesPort()),
                instanceConfig.getConnectionsHealth(), 5_000, 100);

            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = deployable.getTenantRoutingHttpClientInitializer();

            AmzaCatwalkConfig amzaCatwalkConfig = deployable.config(AmzaCatwalkConfig.class);
            TenantAwareHttpClient<String> amzaClient = tenantRoutingHttpClientInitializer.builder(
                tenantRoutingProvider.getConnections("amza", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .debugClient(amzaCatwalkConfig.getAmzaDebugClientCount(), amzaCatwalkConfig.getAmzaDebugClientCountInterval())
                .build(); // TODO expose to conf

            TenantAwareHttpClient<String> manageHttpClient = tenantRoutingHttpClientInitializer.builder(
                tenantRoutingProvider.getConnections("miru-manage", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build(); // TODO expose to conf
            TenantAwareHttpClient<String> readerClient = tenantRoutingHttpClientInitializer.builder(
                tenantRoutingProvider.getConnections("miru-reader", "main", 10_000), // TODO config
                clientHealthProvider)
                .deadAfterNErrors(10)
                .checkDeadEveryNMillis(10_000)
                .build(); // TODO expose to conf

            SickThreads walClientSickThreads = new SickThreads();
            deployable.addHealthCheck(new SickThreadsHealthCheck(deployable.config(WALClientSickThreadsHealthCheckConfig.class), walClientSickThreads));

            AmzaService amzaService = new MiruAmzaServiceInitializer().initialize(deployable,
                clientHealthProvider,
                instanceConfig.getInstanceName(),
                instanceConfig.getInstanceKey(),
                instanceConfig.getServiceName(),
                instanceConfig.getDatacenter(),
                instanceConfig.getRack(),
                instanceConfig.getHost(),
                instanceConfig.getMainPort(),
                null, //"amza-topology-" + instanceConfig.getClusterName(), // Manual service discovery if null
                amzaCatwalkConfig,
                true,
                rowsChanged -> {
                });

            BAInterner baInterner = new BAInterner();
            AmzaClientProvider<HttpClient, HttpClientException> amzaClientProvider = new AmzaClientProvider<>(
                new HttpPartitionClientFactory(baInterner),
                new HttpPartitionHostsProvider(baInterner, amzaClient, mapper),
                new RingHostHttpClientProvider(amzaClient),
                Executors.newFixedThreadPool(amzaCatwalkConfig.getAmzaCallerThreadPoolSize()), //TODO expose to conf
                amzaCatwalkConfig.getAmzaAwaitLeaderElectionForNMillis(),
                amzaCatwalkConfig.getAmzaDebugClientCount(),
                amzaCatwalkConfig.getAmzaDebugClientCountInterval());

            EmbeddedClientProvider embeddedClientProvider = new EmbeddedClientProvider(amzaService);

            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);

            MiruStats stats = new MiruStats();

            int numProcs = Runtime.getRuntime().availableProcessors();
            ScheduledExecutorService queueConsumers = Executors.newScheduledThreadPool(numProcs, new ThreadFactoryBuilder().setNameFormat("queueConsumers-%d")
                .build());
            ExecutorService modelUpdaters = Executors.newFixedThreadPool(numProcs, new ThreadFactoryBuilder().setNameFormat("modelUpdaters-%d").build());
            ExecutorService readRepairers = Executors.newFixedThreadPool(numProcs, new ThreadFactoryBuilder().setNameFormat("readRepairers-%d").build());

            MiruTenantQueryRouting tenantQueryRouting = new MiruTenantQueryRouting();

            CatwalkModelQueue catwalkModelQueue = new CatwalkModelQueue(amzaService,
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
                readerClient,
                mapper,
                responseMapper,
                modelUpdaters,
                amzaService,
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
                //.addResourcePath("../../../../../src/main/resources") // fluff?
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setDirectoryListingAllowed(false)
                .setContext("/ui/static");

            AuthValidationFilter authValidationFilter = new AuthValidationFilter(deployable)
                .addNoAuth("/amza/*"); //TODO delegate to amza
            if (instanceConfig.getMainServiceAuthEnabled()) {
                authValidationFilter.addSessionAuth("/ui/*", "/miru/*");
                authValidationFilter.addRouteOAuth("/miru/*");
            } else {
                authValidationFilter.addSessionAuth("/ui/*");
                authValidationFilter.addNoAuth("/miru/*");
            }
            deployable.addContainerRequestFilter(authValidationFilter);

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
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
