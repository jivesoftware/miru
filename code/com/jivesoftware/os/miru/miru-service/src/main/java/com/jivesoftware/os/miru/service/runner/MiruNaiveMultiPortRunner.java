package com.jivesoftware.os.miru.service.runner;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.jivesoftware.jive.deployer.deployable.service.add.ons.ShutdownHook;
import com.jivesoftware.jive.deployer.server.http.jetty.jersey.server.InitializeRestfulServer.RestfulServerConfig;
import com.jivesoftware.jive.deployer.server.http.jetty.jersey.server.RestfulManageServer;
import com.jivesoftware.jive.entitlements.entitlements.api.EntitlementExpressionProvider;
import com.jivesoftware.jive.identity.identity.client.IdentityClientHttpClientInitializer.IdentityClientHttpClientConfig;
import com.jivesoftware.jive.identity.identity.client.KeyStoreConfig;
import com.jivesoftware.jive.platform.configurator.configurable.defaults.ServiceConfiguration;
import com.jivesoftware.os.miru.admin.MiruAdminServiceConfig;
import com.jivesoftware.os.miru.cluster.MiruRegistryConfig;
import com.jivesoftware.os.miru.reader.MiruHttpClientReaderConfig;
import com.jivesoftware.os.miru.service.MiruReaderConfig;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.MiruWriterConfig;
import com.jivesoftware.jive.platform.reader.reader.client.ReaderServiceHttpClientInitializer.ReaderServiceHttpClientConfig;
import com.jivesoftware.jive.platform.shared.quorum.config.HBaseClientConfig;
import com.jivesoftware.jive.ui.shared.render.guice.RenderHttpClientConfig;
import java.util.List;
import javax.annotation.Nullable;

/**
 *
 */
public class MiruNaiveMultiPortRunner implements MiruRunner {

    @Override
    public void run(RestfulServerConfig restfulServerConfig,
        ServiceConfiguration serviceConfig,
        ReaderServiceHttpClientConfig readerServiceHttpClientConfig,
        IdentityClientHttpClientConfig identityClientHttpClientConfig,
        KeyStoreConfig verifyKeyStoreConfig,
        MiruServiceConfig miruServiceConfig,
        HBaseClientConfig hBaseClientConfig,
        MiruReaderConfig miruReaderConfig,
        MiruWriterConfig miruWriterConfig,
        MiruRegistryConfig miruRegistryConfig,
        MiruAdminServiceConfig miruAdminServiceConfig,
        MiruHttpClientReaderConfig miruHttpClientReaderConfig,
        EntitlementExpressionProvider entitlementExpressionProvider,
        RenderHttpClientConfig renderConfig,
        RestfulManageServer restfulManageServer,
        ShutdownHook shutdownHook) throws Exception {

        MiruRestfulServerConfigRunner configRunner = new MiruRestfulServerConfigRunner();

        for (int port : getRunnerPorts(miruServiceConfig)) {
            configRunner.run(restfulServerConfig, remapPort(serviceConfig, port), readerServiceHttpClientConfig, identityClientHttpClientConfig,
                verifyKeyStoreConfig, miruServiceConfig, hBaseClientConfig, miruReaderConfig, miruWriterConfig, miruRegistryConfig, miruAdminServiceConfig,
                miruHttpClientReaderConfig, entitlementExpressionProvider, renderConfig, restfulManageServer, shutdownHook);
        }
    }

    private List<Integer> getRunnerPorts(MiruServiceConfig miruServiceConfig) {
        return Lists.transform(Lists.newArrayList(miruServiceConfig.getRunnerPorts().split("\\s*,\\s*")), new Function<String, Integer>() {
            @Nullable
            @Override
            public Integer apply(@Nullable String input) {
                return new Integer(input);
            }
        });
    }

    private ServiceConfiguration remapPort(final ServiceConfiguration delegate, final int port) {
        return new ServiceConfiguration() {
            @Override
            public Integer getManagePort() {
                return delegate.getManagePort();
            }

            @Override
            public Integer getPort() {
                return port;
            }

            @Override
            public Integer getShutdownTimeout() {
                return delegate.getShutdownTimeout();
            }

            @Override
            public Integer getStartupTimeout() {
                return delegate.getStartupTimeout();
            }

            @Override
            public Integer getArchiveCount() {
                return delegate.getArchiveCount();
            }

            @Override
            public Integer getReleaseCount() {
                return delegate.getReleaseCount();
            }

            @Override
            public String getServiceOkToDeploy() {
                return delegate.getServiceOkToDeploy();
            }

            @Override
            public String getServiceName() {
                return delegate.getServiceName();
            }

            @Override
            public String getServiceVersion() {
                return delegate.getServiceVersion();
            }

            @Override
            public String getServiceTags() {
                return delegate.getServiceTags();
            }

            @Override
            public String getServicePhase() {
                return delegate.getServicePhase();
            }

            @Override
            public String getHostName() {
                return delegate.getHostName();
            }

            @Override
            public String getClusterName() {
                return delegate.getClusterName();
            }

            @Override
            public String getInstanceName() {
                return delegate.getInstanceName();
            }

            @Override
            public String getDataCenterName() {
                return delegate.getDataCenterName();
            }

            @Override
            public String name() {
                return delegate.name();
            }

            @Override
            public void applyDefaults() {
                delegate.applyDefaults();
            }
        };
    }
}
