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
package com.jivesoftware.os.miru.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.hbase.HBaseSetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.hbase.HBaseSetOfSortedMapsImplInitializer.HBaseSetOfSortedMapsConfig;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.api.MiruReader;
import com.jivesoftware.os.miru.api.MiruWriter;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.MiruRegistryStoreInitializer;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.service.reader.MiruReaderImpl;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.MiruServiceInitializer;
import com.jivesoftware.os.miru.service.writer.MiruWriterImpl;
import com.jivesoftware.os.miru.service.endpoint.MiruConfigEndpoints;
import com.jivesoftware.os.miru.service.endpoint.MiruReaderEndpoints;
import com.jivesoftware.os.miru.service.endpoint.MiruWriterEndpoints;
import com.jivesoftware.os.miru.service.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.service.schema.MiruSchema;
import com.jivesoftware.os.miru.service.stream.locator.MiruResourceLocatorProvider;
import com.jivesoftware.os.miru.service.stream.locator.MiruResourceLocatorProviderInitializer;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.upena.main.Deployable;
import com.jivesoftware.os.upena.main.InstanceConfig;
import com.jivesoftware.os.upena.routing.shared.TenantsServiceConnectionDescriptorProvider;
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantRoutingHttpClient;
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantRoutingHttpClientInitializer;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class MiruMain {

    public static void main(String[] args) throws Exception {
        new MiruMain().run(args);
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

        MiruLifecyle<MiruResourceLocatorProvider> miruResourceLocatorProviderLifecyle = new MiruResourceLocatorProviderInitializer()
                .initialize(miruServiceConfig);

        miruResourceLocatorProviderLifecyle.start();
        MiruLifecyle<MiruService> miruServiceLifecyle = new MiruServiceInitializer().initialize(miruServiceConfig,
                registryStore,
                clusterRegistry,
                miruHost,
                new MiruSchema(DefaultMiruSchemaDefinition.SCHEMA),
                wal,
                httpClientFactory,
                miruResourceLocatorProviderLifecyle.getService());

        miruServiceLifecyle.start();
        MiruService miruService = miruServiceLifecyle.getService();
        MiruReader miruReader = new MiruReaderImpl(miruService);
        MiruWriter miruWriter = new MiruWriterImpl(miruService);

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());

        deployable.addEndpoints(MiruWriterEndpoints.class);
        deployable.addInjectables(MiruWriter.class, miruWriter);
        deployable.addEndpoints(MiruReaderEndpoints.class);
        deployable.addInjectables(MiruReader.class, miruReader);
        deployable.addEndpoints(MiruConfigEndpoints.class);
        deployable.addInjectables(MiruClusterRegistry.class, clusterRegistry);
        deployable.addInjectables(MiruService.class, miruService);

        deployable.buildServer().start();

    }
}
