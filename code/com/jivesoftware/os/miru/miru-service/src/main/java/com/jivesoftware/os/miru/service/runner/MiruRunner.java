package com.jivesoftware.os.miru.service.runner;

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

/**
 *
 */
public interface MiruRunner {

    void run(RestfulServerConfig restfulServerConfig,
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
        ShutdownHook shutdownHook) throws Exception;
}
