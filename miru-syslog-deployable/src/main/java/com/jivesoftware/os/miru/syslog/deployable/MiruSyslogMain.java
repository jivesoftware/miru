package com.jivesoftware.os.miru.syslog.deployable;

import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer.MiruLogAppenderConfig;
import com.jivesoftware.os.miru.logappender.RoutingBirdLogSenderProvider;
import com.jivesoftware.os.miru.syslog.deployable.MiruSyslogIntakeInitializer.MiruSyslogIntakeConfig;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.deployable.DeployableHealthCheckRegistry;
import com.jivesoftware.os.routing.bird.deployable.ErrorHealthCheckConfig;
import com.jivesoftware.os.routing.bird.deployable.InstanceConfig;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.health.checkers.FileDescriptorCountHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.GCPauseHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.LoadAverageHealthChecker;
import com.jivesoftware.os.routing.bird.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.routing.bird.health.checkers.SystemCpuHealthChecker;
import com.jivesoftware.os.routing.bird.http.client.HttpDeliveryClientHealthProvider;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;

public class MiruSyslogMain {

    public static void main(String[] args) throws Exception {
        new MiruSyslogMain().run(args);
    }

    public void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();

        try {
            final Deployable deployable = new Deployable(args);

            HealthFactory.initialize(deployable::config, new DeployableHealthCheckRegistry(deployable));
            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);

            deployable.addHealthCheck(new GCPauseHealthChecker(deployable.config(GCPauseHealthChecker.GCPauseHealthCheckerConfig.class)));
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(new SystemCpuHealthChecker(deployable.config(SystemCpuHealthChecker.SystemCpuHealthCheckerConfig.class)));
            deployable.addHealthCheck(new LoadAverageHealthChecker(deployable.config(LoadAverageHealthChecker.LoadAverageHealthCheckerConfig.class)));
            deployable.addHealthCheck(new FileDescriptorCountHealthChecker(deployable.config(FileDescriptorCountHealthChecker.FileDescriptorCountHealthCheckerConfig.class)));
            deployable.addHealthCheck(serviceStartupHealthCheck);
            deployable.addErrorHealthChecks(deployable.config(ErrorHealthCheckConfig.class));
            deployable.buildManageServer().start();

            MiruSyslogConfig miruSyslogConfig = deployable.config(MiruSyslogConfig.class);
            MiruSyslogIntakeConfig miruSyslogIntakeConfig = deployable.config(MiruSyslogIntakeConfig.class);
            MiruLogAppenderConfig miruLogAppenderConfig = deployable.config(MiruLogAppenderConfig.class);

            @SuppressWarnings("unchecked")
            RoutingBirdLogSenderProvider logSenderProviderAppender = new RoutingBirdLogSenderProvider<>(
                    deployable.getTenantRoutingProvider().getConnections(
                            "miru-stumptown",
                            "main",
                            miruSyslogConfig.getRefreshConnectionsAfterNMillis()),
                    miruSyslogConfig.getTenant(),
                    miruLogAppenderConfig.getSocketTimeoutInMillis());
            new MiruLogAppenderInitializer().initialize(
                    instanceConfig.getDatacenter(),
                    instanceConfig.getClusterName(),
                    instanceConfig.getHost(),
                    instanceConfig.getServiceName(),
                    String.valueOf(instanceConfig.getInstanceName()),
                    instanceConfig.getVersion(),
                    miruLogAppenderConfig,
                    logSenderProviderAppender).install();

            @SuppressWarnings("unchecked")
            RoutingBirdLogSenderProvider logSenderProviderIntake = new RoutingBirdLogSenderProvider<>(
                    deployable.getTenantRoutingProvider().getConnections(
                            "miru-stumptown",
                            "main",
                            miruSyslogConfig.getRefreshConnectionsAfterNMillis()),
                    miruSyslogConfig.getTenant(),
                    miruLogAppenderConfig.getSocketTimeoutInMillis());
            new MiruSyslogIntakeInitializer().initialize(
                    instanceConfig,
                    miruSyslogIntakeConfig,
                    miruLogAppenderConfig,
                    logSenderProviderIntake).start();

            HttpDeliveryClientHealthProvider clientHealthProvider = new HttpDeliveryClientHealthProvider(
                    instanceConfig.getInstanceKey(),
                    HttpRequestHelperUtils.buildRequestHelper(false, false, null, instanceConfig.getRoutesHost(), instanceConfig.getRoutesPort()),
                    instanceConfig.getConnectionsHealth(),
                    miruSyslogConfig.getHealthIntervalNMillis(),
                    miruSyslogConfig.getHealthSampleWindow());
            clientHealthProvider.start();

            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }

}
