package com.jivesoftware.os.miru.service.runner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.jivesoftware.jive.deployer.deployable.service.add.ons.ShutdownHook;
import com.jivesoftware.jive.deployer.server.http.jetty.jersey.endpoints.testable.SelfTestHealthCheck;
import com.jivesoftware.jive.deployer.server.http.jetty.jersey.endpoints.testable.TestableEndpoints;
import com.jivesoftware.jive.deployer.server.http.jetty.jersey.server.InitializeRestfulServer;
import com.jivesoftware.jive.deployer.server.http.jetty.jersey.server.InitializeRestfulServer.RestfulServerConfig;
import com.jivesoftware.jive.deployer.server.http.jetty.jersey.server.JerseyEndpoints;
import com.jivesoftware.jive.deployer.server.http.jetty.jersey.server.RestfulManageServer;
import com.jivesoftware.jive.deployer.server.http.jetty.jersey.server.binding.Injectable;
import com.jivesoftware.jive.deployer.server.http.jetty.jersey.server.util.ResourceImpl;
import com.jivesoftware.jive.entitlements.entitlements.api.EntitlementExpressionProvider;
import com.jivesoftware.jive.identity.identity.client.IdentityClient;
import com.jivesoftware.jive.identity.identity.client.IdentityClientHttpClientInitializer;
import com.jivesoftware.jive.identity.identity.client.IdentityClientHttpClientInitializer.IdentityClientHttpClientConfig;
import com.jivesoftware.jive.identity.identity.client.KeyStoreConfig;
import com.jivesoftware.jive.platform.configurator.configurable.defaults.ServiceConfiguration;
import com.jivesoftware.os.miru.admin.MiruAdminEndpoints;
import com.jivesoftware.os.miru.admin.MiruAdminService;
import com.jivesoftware.os.miru.admin.MiruAdminServiceConfig;
import com.jivesoftware.os.miru.admin.MiruAdminServiceInitializer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.MiruWriter;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryConfig;
import com.jivesoftware.os.miru.cluster.MiruRegistryInitializer;
import com.jivesoftware.os.miru.reader.MiruHttpClientReaderConfig;
import com.jivesoftware.os.miru.service.MiruReaderConfig;
import com.jivesoftware.os.miru.service.MiruReaderInitializer;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.MiruServiceInitializer;
import com.jivesoftware.os.miru.service.MiruWriterConfig;
import com.jivesoftware.os.miru.service.MiruWriterInitializer;
import com.jivesoftware.os.miru.service.endpoint.MiruConfigEndpoints;
import com.jivesoftware.os.miru.service.endpoint.MiruReaderEndpoints;
import com.jivesoftware.os.miru.service.endpoint.MiruWriterEndpoints;
import com.jivesoftware.os.miru.service.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.service.schema.MiruSchema;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.jive.platform.reader.reader.client.ReaderService;
import com.jivesoftware.jive.platform.reader.reader.client.ReaderServiceHttpClientInitializer;
import com.jivesoftware.jive.platform.reader.reader.client.ReaderServiceHttpClientInitializer.ReaderServiceHttpClientConfig;
import com.jivesoftware.jive.platform.shared.quorum.config.HBaseClientConfig;
import com.jivesoftware.jive.platform.shared.quorum.config.HBaseClientConfigConverter;
import com.jivesoftware.jive.ui.shared.render.core.request.RequestInfoFilter;
import com.jivesoftware.jive.ui.shared.render.core.request.RequestInfoInjectable;
import com.jivesoftware.jive.ui.shared.render.guice.RenderHttpClientConfig;
import com.jivesoftware.os.jive.utils.base.service.ServiceHandle;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.hbase.HBaseSetOfSortedMapsImplInitializer;
import java.util.List;

/**
 *
 */
public class MiruRestfulServerConfigRunner implements MiruRunner {

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

        MiruHost miruHost = new MiruHost(serviceConfig.getHostName(), serviceConfig.getPort());

        SetOfSortedMapsImplInitializer<Exception> setOfSortedMapsInitializer = new HBaseSetOfSortedMapsImplInitializer(
            HBaseClientConfigConverter.getHBaseSetOfSortedMapsConfig(hBaseClientConfig));

        MiruRegistryInitializer miruRegistryInitializer = MiruRegistryInitializer.initialize(miruRegistryConfig,
            hBaseClientConfig.getTablePrefix(),
            setOfSortedMapsInitializer,
            miruServiceConfig.getClusterRegistryClass());

        MiruWALInitializer miruWALInitializer = MiruWALInitializer.initialize(hBaseClientConfig.getTablePrefix(),
            setOfSortedMapsInitializer,
            miruServiceConfig.getActivityWALReaderClass(),
            miruServiceConfig.getActivityWALWriterClass(),
            miruServiceConfig.getReadTrackingWALReaderClass(),
            miruServiceConfig.getReadTrackingWALWriterClass());

        MiruServiceInitializer miruServiceInitializer = MiruServiceInitializer
            .initialize(miruServiceConfig, miruRegistryInitializer, miruWALInitializer, miruHost, new MiruSchema(DefaultMiruSchemaDefinition.SCHEMA),
                miruHttpClientReaderConfig);
        MiruService miruService = miruServiceInitializer.getMiruService();

        MiruReaderInitializer miruReaderInitializer = MiruReaderInitializer.initialize(miruReaderConfig, miruService, entitlementExpressionProvider);
        MiruReader miruReader = miruReaderInitializer.getMiruReader();
        MiruWriterInitializer miruWriterInitializer = MiruWriterInitializer.initialize(miruWriterConfig, miruService);
        MiruWriter miruWriter = miruWriterInitializer.getMiruWriter();

        ReaderService readerService = new ReaderServiceHttpClientInitializer().initialize(readerServiceHttpClientConfig);
        MiruAdminService miruAdminService = new MiruAdminServiceInitializer().initialize(miruAdminServiceConfig, miruServiceInitializer.getClusterRegistry(),
            miruServiceInitializer.getActivityWALReader(), miruServiceInitializer.getReadTrackingWALReader(), miruServiceInitializer.getActivityLookupTable(),
            readerService, renderConfig);
        IdentityClient identityClient = new IdentityClientHttpClientInitializer().initialize(identityClientHttpClientConfig, verifyKeyStoreConfig);


        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());

        JerseyEndpoints jerseyEndpoints = new JerseyEndpoints(mapper)
            .enableTracing()
            .addEndpoint(MiruWriterEndpoints.class).addInjectable(MiruWriter.class, miruWriter)
            .addEndpoint(MiruReaderEndpoints.class).addInjectable(MiruReader.class, miruReader)
            .addEndpoint(MiruConfigEndpoints.class)
            .addInjectable(MiruClusterRegistry.class, miruRegistryInitializer.getClusterRegistry())
            .addInjectable(MiruService.class, miruService);

        JerseyEndpoints adminEndpoints = new JerseyEndpoints()
            .enableCORS()
            .enableTracing()
            .addBinder(new RequestInfoInjectable(identityClient))
            .addProvider(RequestInfoFilter.class)
            .addEndpoint(MiruAdminEndpoints.class).addInjectable(miruAdminService);

        ResourceImpl sourceTree = new ResourceImpl()
            .addResourcePath("../../../../../src/main/resources")
            .addResourcePath("resources")
            .setContext("/static");

        List<Injectable<?>> injectables = jerseyEndpoints.getInjectables();
        TestableEndpoints testableEndpoints = new TestableEndpoints(injectables);
        restfulManageServer.addHealthCheck(new SelfTestHealthCheck(injectables));

        ServiceHandle serviceHandle = new InitializeRestfulServer(serviceConfig, restfulServerConfig)
            .addContextHandler("/test", testableEndpoints)
            .addContextHandler("/", jerseyEndpoints)
            .addContextHandler("/miru/admin", adminEndpoints)
            .addResource(sourceTree)
            .build();
        serviceHandle.start();

        shutdownHook.register(serviceHandle);
    }
}
