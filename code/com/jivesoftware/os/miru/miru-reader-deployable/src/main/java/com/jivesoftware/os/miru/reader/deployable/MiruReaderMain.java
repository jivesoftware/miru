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
package com.jivesoftware.os.miru.reader.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.Interners;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.hbase.HBaseSetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.hbase.HBaseSetOfSortedMapsImplInitializer.HBaseSetOfSortedMapsConfig;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.api.MiruWriter;
import com.jivesoftware.os.miru.api.activity.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.SingleSchemaProvider;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.MiruRegistryStoreInitializer;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.query.Miru;
import com.jivesoftware.os.miru.query.MiruActivityInternExtern;
import com.jivesoftware.os.miru.query.MiruJustInTimeBackfillerizer;
import com.jivesoftware.os.miru.query.MiruProvider;
import com.jivesoftware.os.miru.query.SingleBitmapsProvider;
import com.jivesoftware.os.miru.query.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.query.plugin.MiruPlugin;
import com.jivesoftware.os.miru.service.MiruBackfillerizerInitializer;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.MiruServiceInitializer;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.service.endpoint.MiruConfigEndpoints;
import com.jivesoftware.os.miru.service.endpoint.MiruReaderEndpoints;
import com.jivesoftware.os.miru.service.endpoint.MiruWriterEndpoints;
import com.jivesoftware.os.miru.service.stream.locator.MiruResourceLocatorProvider;
import com.jivesoftware.os.miru.service.stream.locator.MiruResourceLocatorProviderInitializer;
import com.jivesoftware.os.miru.service.writer.MiruWriterImpl;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.upena.main.Deployable;
import com.jivesoftware.os.upena.main.InstanceConfig;
import com.jivesoftware.os.upena.routing.shared.TenantsServiceConnectionDescriptorProvider;
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantRoutingHttpClient;
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantRoutingHttpClientInitializer;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

public class MiruReaderMain {

    public static void main(String[] args) throws Exception {
        new MiruReaderMain().run(args);
    }

    public void run(String[] args) throws Exception {

        Deployable deployable = new Deployable(args);
        deployable.buildManageServer().start();

        TenantsServiceConnectionDescriptorProvider connections = deployable.getTenantRoutingProvider().getConnections("miru-deployable", "main");
        TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = new TenantRoutingHttpClientInitializer<>();
        TenantRoutingHttpClient<String> client = tenantRoutingHttpClientInitializer.initialize(connections);

        InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);
        MiruHost miruHost = new MiruHost(instanceConfig.getHost(), instanceConfig.getMainPort());

        HBaseSetOfSortedMapsConfig hbaseConfig = deployable.config(HBaseSetOfSortedMapsConfig.class);
        SetOfSortedMapsImplInitializer<Exception> setOfSortedMapsInitializer = new HBaseSetOfSortedMapsImplInitializer(hbaseConfig);

        MiruServiceConfig miruServiceConfig = deployable.config(MiruServiceConfig.class);

        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
                .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList());

        MiruRegistryStore registryStore = new MiruRegistryStoreInitializer().initialize(instanceConfig.getClusterName(), setOfSortedMapsInitializer);
        MiruClusterRegistry clusterRegistry = new MiruRCVSClusterRegistry(registryStore.getHostsRegistry(),
                registryStore.getExpectedTenantsRegistry(),
                registryStore.getExpectedTenantPartitionsRegistry(),
                registryStore.getReplicaRegistry(),
                registryStore.getTopologyRegistry(),
                registryStore.getConfigRegistry(),
                3,
                TimeUnit.HOURS.toMillis(1));

        MiruWALInitializer.MiruWAL wal = new MiruWALInitializer().initialize(instanceConfig.getClusterName(), setOfSortedMapsInitializer);

        MiruLifecyle<MiruJustInTimeBackfillerizer> backfillerizerLifecycle = new MiruBackfillerizerInitializer().initialize(miruServiceConfig, miruHost);

        backfillerizerLifecycle.start();
        final MiruJustInTimeBackfillerizer backfillerizer = backfillerizerLifecycle.getService();

        MiruLifecyle<MiruResourceLocatorProvider> miruResourceLocatorProviderLifecyle = new MiruResourceLocatorProviderInitializer()
                .initialize(miruServiceConfig);

        miruResourceLocatorProviderLifecyle.start();

        final MiruActivityInternExtern internExtern = new MiruActivityInternExtern(
                Interners.<MiruIBA>newWeakInterner(),
                Interners.<MiruTermId>newWeakInterner(),
                Interners.<MiruTenantId>newStrongInterner(),
                // makes sense to share string internment as this is authz in both cases
                Interners.<String>newWeakInterner());
        final MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruLifecyle<MiruService> miruServiceLifecyle = new MiruServiceInitializer().initialize(miruServiceConfig,
                registryStore,
                clusterRegistry,
                miruHost,
                new SingleSchemaProvider(new MiruSchema(DefaultMiruSchemaDefinition.FIELDS)),
                wal,
                httpClientFactory,
                miruResourceLocatorProviderLifecyle.getService(),
                internExtern,
                new SingleBitmapsProvider<>(bitmaps));

        miruServiceLifecyle.start();
        final MiruService miruService = miruServiceLifecyle.getService();
        MiruWriter miruWriter = new MiruWriterImpl(miruService);

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());

        deployable.addEndpoints(MiruWriterEndpoints.class);
        deployable.addInjectables(MiruWriter.class, miruWriter);
        deployable.addEndpoints(MiruReaderEndpoints.class);
        deployable.addInjectables(MiruService.class, miruService);

        MiruProvider<Miru> miruProvider = new MiruProvider<Miru>() {
            @Override
            public Miru getMiru(MiruTenantId tenantId) {
                return miruService;
            }

            @Override
            public MiruActivityInternExtern getActivityInternExtern(MiruTenantId tenantId) {
                return internExtern;
            }

            @Override
            public MiruJustInTimeBackfillerizer getBackfillerizer(MiruTenantId tenantId) {
                return backfillerizer;
            }
        };

        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage("com.jivesoftware")) // GRRR
                .setScanners(new SubTypesScanner(), new TypesScanner()));
        Set<Class<? extends MiruPlugin>> pluginTypes = reflections.getSubTypesOf(MiruPlugin.class);
        for (Class<? extends MiruPlugin> pluginType : pluginTypes) {
            add(miruProvider, deployable, pluginType.newInstance());
        }

        deployable.addEndpoints(MiruConfigEndpoints.class);
        deployable.addInjectables(MiruClusterRegistry.class, clusterRegistry);
        deployable.addInjectables(MiruService.class, miruService);

        deployable.buildServer().start();

    }

    private <E, I> void add(MiruProvider miruProvider, Deployable deployable, MiruPlugin<E, I> plugin) {
        Class<E> endpointsClass = plugin.getEndpointsClass();
        deployable.addEndpoints(endpointsClass);
        Collection<MiruEndpointInjectable<I>> injectables = plugin.getInjectables(miruProvider);
        for (MiruEndpointInjectable<?> miruEndpointInjectable : injectables) {
            deployable.addInjectables(miruEndpointInjectable.getInjectableClass(), miruEndpointInjectable.getInjectable());
        }
    }
}
