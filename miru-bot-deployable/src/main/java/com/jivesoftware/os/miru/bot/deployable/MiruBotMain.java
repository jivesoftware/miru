package com.jivesoftware.os.miru.bot.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.bot.deployable.MiruBotDistinctsInitializer.MiruBotDistinctsConfig;
import com.jivesoftware.os.miru.bot.deployable.MiruBotHealthCheck.MiruBotHealthCheckConfig;
import com.jivesoftware.os.miru.bot.deployable.MiruBotUniquesInitializer.MiruBotUniquesConfig;
import com.jivesoftware.os.miru.cluster.client.MiruClusterClientInitializer;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer.MiruLogAppenderConfig;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.deployable.DeployableHealthCheckRegistry;
import com.jivesoftware.os.routing.bird.deployable.ErrorHealthCheckConfig;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.endpoints.base.FullyOnlineVersion;
import com.jivesoftware.os.routing.bird.endpoints.base.HasUI;
import com.jivesoftware.os.routing.bird.endpoints.base.LoadBalancerHealthCheckEndpoints;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.checkers.FileDescriptorCountHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCPauseHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.LoadAverageHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.routing.bird.health.checkers.SystemCpuHealthChecker;
import com.jivesoftware.os.routing.bird.http.client.HttpDeliveryClientHealthProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.http.client.TenantRoutingHttpClientInitializer;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MiruBotMain {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static void main(String[] args) throws Exception {
        new MiruBotMain().run(args);
    }

    private void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();

        try {
            final Deployable deployable = new Deployable(args);
            HealthFactory.initialize(deployable::config, new DeployableHealthCheckRegistry(deployable));
            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList()));

            MiruBotHealthCheck miruBotHealthCheck =
                new MiruBotHealthCheck(deployable.config(MiruBotHealthCheckConfig.class));

            deployable.addHealthCheck(
                new GCPauseHealthChecker(deployable.config(GCPauseHealthChecker.GCPauseHealthCheckerConfig.class)),
                new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)),
                new SystemCpuHealthChecker(deployable.config(SystemCpuHealthChecker.SystemCpuHealthCheckerConfig.class)),
                new LoadAverageHealthChecker(deployable.config(LoadAverageHealthChecker.LoadAverageHealthCheckerConfig.class)),
                new FileDescriptorCountHealthChecker(deployable.config(FileDescriptorCountHealthChecker.FileDescriptorCountHealthCheckerConfig.class)),
                miruBotHealthCheck,
                serviceStartupHealthCheck);
            deployable.addErrorHealthChecks(deployable.config(ErrorHealthCheckConfig.class));
            deployable.addManageInjectables(FullyOnlineVersion.class, (FullyOnlineVersion) () -> {
                if (serviceStartupHealthCheck.startupHasSucceeded()) {
                    return instanceConfig.getVersion();
                } else {
                    return null;
                }
            });
            deployable.buildManageServer().start();

            MiruBotConfig miruBotConfig = deployable.config(MiruBotConfig.class);
            LOG.info("Dead after n errors: {}", miruBotConfig.getDeadAfterNErrors());
            LOG.info("Check dead every nms: {}", miruBotConfig.getCheckDeadEveryNMillis());
            LOG.info("Refresh connections after nms: {}", miruBotConfig.getRefreshConnectionsAfterNMillis());
            LOG.info("Health interval: {}", miruBotConfig.getHealthInterval());
            LOG.info("Health sample window: {}", miruBotConfig.getHealthSampleWindow());
            LOG.info("Miru ingress endpoint: {}", miruBotConfig.getMiruIngressEndpoint());

            HttpDeliveryClientHealthProvider clientHealthProvider = new HttpDeliveryClientHealthProvider(
                instanceConfig.getInstanceKey(),
                HttpRequestHelperUtils.buildRequestHelper(false, false, null, instanceConfig.getRoutesHost(), instanceConfig.getRoutesPort()),
                instanceConfig.getConnectionsHealth(),
                miruBotConfig.getHealthInterval(),
                miruBotConfig.getHealthSampleWindow());

            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer =
                deployable.getTenantRoutingHttpClientInitializer();

            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> miruWriterClient = tenantRoutingHttpClientInitializer.builder(
                deployable.getTenantRoutingProvider().getConnections(
                    "miru-writer",
                    "main",
                    miruBotConfig.getRefreshConnectionsAfterNMillis()),
                clientHealthProvider)
                .deadAfterNErrors(miruBotConfig.getDeadAfterNErrors())
                .checkDeadEveryNMillis(miruBotConfig.getCheckDeadEveryNMillis())
                .build();

            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> miruManageClient = tenantRoutingHttpClientInitializer.builder(
                deployable.getTenantRoutingProvider().getConnections(
                    "miru-manage",
                    "main",
                    miruBotConfig.getRefreshConnectionsAfterNMillis()),
                clientHealthProvider)
                .deadAfterNErrors(miruBotConfig.getDeadAfterNErrors())
                .checkDeadEveryNMillis(miruBotConfig.getCheckDeadEveryNMillis())
                .build();

            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> miruReaderClient = tenantRoutingHttpClientInitializer.builder(
                deployable.getTenantRoutingProvider().getConnections(
                    "miru-reader",
                    "main",
                    miruBotConfig.getRefreshConnectionsAfterNMillis()),
                clientHealthProvider)
                .deadAfterNErrors(miruBotConfig.getDeadAfterNErrors())
                .checkDeadEveryNMillis(miruBotConfig.getCheckDeadEveryNMillis())
                .build();

            MiruLogAppenderConfig miruLogAppenderConfig =
                deployable.config(MiruLogAppenderConfig.class);
            @SuppressWarnings("unchecked")
            TenantAwareHttpClient<String> miruStumptownClient = tenantRoutingHttpClientInitializer.builder(
                deployable.getTenantRoutingProvider().getConnections(
                    "miru-stumptown",
                    "main",
                    miruBotConfig.getRefreshConnectionsAfterNMillis()),
                clientHealthProvider)
                .deadAfterNErrors(miruBotConfig.getDeadAfterNErrors())
                .checkDeadEveryNMillis(miruBotConfig.getCheckDeadEveryNMillis())
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

            MiruBotDistinctsConfig miruBotDistinctsConfig = deployable.config(MiruBotDistinctsConfig.class);
            MiruBotUniquesConfig miruBotUniquesConfig = deployable.config(MiruBotUniquesConfig.class);

            ExecutorService tasExecutors = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("tas-%d").build());

            MiruClusterClient miruClusterClient = new MiruClusterClientInitializer(tasExecutors, 100, 95).initialize(
                new MiruStats(), "", miruManageClient, new ObjectMapper());

            MiruBotSchemaService miruBotSchemaService = new MiruBotSchemaService(miruClusterClient);

            OrderIdProvider orderIdProvider = new OrderIdProviderImpl(
                new ConstantWriterIdProvider(instanceConfig.getInstanceName()));

            MiruBotDistinctsService miruBotDistinctsService = new MiruBotDistinctsInitializer().initialize(
                miruBotConfig,
                miruBotDistinctsConfig,
                orderIdProvider,
                miruBotSchemaService,
                miruReaderClient,
                miruWriterClient);
            miruBotHealthCheck.addMiruBotHealthPercenter(miruBotDistinctsService);
            miruBotDistinctsService.start();

            MiruBotUniquesService miruBotUniquesService = new MiruBotUniquesInitializer().initialize(
                miruBotConfig,
                miruBotUniquesConfig,
                orderIdProvider,
                miruBotSchemaService,
                miruReaderClient,
                miruWriterClient);
            miruBotHealthCheck.addMiruBotHealthPercenter(miruBotUniquesService);
            miruBotUniquesService.start();

            deployable.addInjectables(MiruBotDistinctsService.class, miruBotDistinctsService);
            deployable.addInjectables(MiruBotUniquesService.class, miruBotUniquesService);
            deployable.addEndpoints(MiruBotBucketEndpoints.class);

            deployable.addEndpoints(LoadBalancerHealthCheckEndpoints.class);
            deployable.buildServer().start();
            clientHealthProvider.start();
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }

}
