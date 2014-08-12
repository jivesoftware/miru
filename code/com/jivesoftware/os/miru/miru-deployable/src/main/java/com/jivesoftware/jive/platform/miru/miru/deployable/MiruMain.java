package com.jivesoftware.os.miru.deployable;

import com.jivesoftware.jive.deployer.deployable.service.add.ons.DeployableMain;
import com.jivesoftware.jive.deployer.deployable.service.add.ons.ShutdownHook;
import com.jivesoftware.jive.deployer.server.http.jetty.jersey.server.InitializeRestfulServer.RestfulServerConfig;
import com.jivesoftware.jive.deployer.server.http.jetty.jersey.server.RestfulManageServer;
import com.jivesoftware.jive.entitlements.entitlements.api.EntitlementExpressionProvider;
import com.jivesoftware.jive.entitlements.entitlements.api.EntitlementSystem;
import com.jivesoftware.jive.entitlements.entitlements.impl.hbase.EntitlementSystemInitializer;
import com.jivesoftware.jive.entitlements.entitlements.impl.hbase.EntitlementSystemInitializer.EntitlementSystemConfig;
import com.jivesoftware.jive.identity.identity.client.IdentityClientHttpClientInitializer.IdentityClientHttpClientConfig;
import com.jivesoftware.jive.identity.identity.client.KeyStoreConfig;
import com.jivesoftware.jive.platform.configurator.configurable.deployed.required.RequiredConfig;
import com.jivesoftware.jive.platform.miru.miru.admin.MiruAdminServiceConfig;
import com.jivesoftware.jive.platform.miru.miru.cluster.MiruRegistryConfig;
import com.jivesoftware.jive.platform.miru.miru.reader.MiruHttpClientReaderConfig;
import com.jivesoftware.jive.platform.miru.miru.service.MiruReaderConfig;
import com.jivesoftware.jive.platform.miru.miru.service.MiruServiceConfig;
import com.jivesoftware.jive.platform.miru.miru.service.MiruWriterConfig;
import com.jivesoftware.jive.platform.miru.miru.service.runner.MiruRunner;
import com.jivesoftware.jive.platform.reader.reader.client.ReaderServiceHttpClientInitializer.ReaderServiceHttpClientConfig;
import com.jivesoftware.jive.platform.shared.quorum.config.HBaseClientConfig;
import com.jivesoftware.jive.ui.shared.render.guice.RenderHttpClientConfig;

public class MiruMain extends DeployableMain {

    @RequiredConfig
    public RestfulServerConfig restfulServerConfig;
    @RequiredConfig
    public HBaseClientConfig hbaseClientConfig;

    // Deployable specific config.
    @RequiredConfig
    public MiruServiceConfig miruServiceConfig;
    @RequiredConfig
    public MiruReaderConfig miruReaderConfig;
    @RequiredConfig
    public MiruWriterConfig miruWriterConfig;
    @RequiredConfig
    public MiruRegistryConfig miruRegistryConfig;
    @RequiredConfig
    public MiruAdminServiceConfig miruAdminServiceConfig;
    @RequiredConfig
    public EntitlementSystemConfig entitlementSystemConfig;
    @RequiredConfig
    public ReaderServiceHttpClientConfig readerServiceConfig;
    @RequiredConfig
    public IdentityClientHttpClientConfig identityClientConfig;
    @RequiredConfig("verify")
    public KeyStoreConfig verifyKeyStoreConfig;
    @RequiredConfig
    public MiruHttpClientReaderConfig miruHttpClientReaderConfig;
    @RequiredConfig
    public RenderHttpClientConfig renderConfig;

    public static void main(String[] args) throws Exception {
        new MiruMain().deployableMain(args);
    }

    @Override
    public void run(RestfulManageServer restfulManageServer, ShutdownHook shutdownHook) throws Exception {

        EntitlementExpressionProvider entitlementExpressionProvider = null;
        if (Boolean.parseBoolean(entitlementSystemConfig.getChecksEnabled())) {
            EntitlementSystem entitlementSystem = EntitlementSystemInitializer.initialize(entitlementSystemConfig);
            entitlementExpressionProvider = entitlementSystem.getExpressionProvider();
        }

        MiruRunner miruRunner = miruServiceConfig.getRunnerClass().newInstance();
        miruRunner.run(restfulServerConfig, serviceConfig, readerServiceConfig, identityClientConfig, verifyKeyStoreConfig, miruServiceConfig,
            hbaseClientConfig, miruReaderConfig, miruWriterConfig, miruRegistryConfig, miruAdminServiceConfig, miruHttpClientReaderConfig,
            entitlementExpressionProvider, renderConfig, restfulManageServer, shutdownHook);
    }
}
