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
package com.jivesoftware.os.miru.writer.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.jivesoftware.os.jive.utils.health.api.HealthCheckConfigBinder;
import com.jivesoftware.os.jive.utils.health.api.HealthCheckRegistry;
import com.jivesoftware.os.jive.utils.health.api.HealthChecker;
import com.jivesoftware.os.jive.utils.health.api.HealthFactory;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.miru.client.MiruClient;
import com.jivesoftware.os.miru.client.MiruClientConfig;
import com.jivesoftware.os.miru.client.MiruClientInitializer;
import com.jivesoftware.os.miru.client.endpoints.MiruClientEndpoints;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryConfig;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.MiruRegistryStoreInitializer;
import com.jivesoftware.os.miru.cluster.MiruReplicaSetDirector;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;
import com.jivesoftware.os.rcvs.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.upena.main.Deployable;
import com.jivesoftware.os.upena.main.InstanceConfig;
import org.merlin.config.Config;

public class MiruWriterMain {

    public static void main(String[] args) throws Exception {
        new MiruWriterMain().run(args);
    }

    public void run(String[] args) throws Exception {

        final Deployable deployable = new Deployable(args);

        HealthFactory.initialize(new HealthCheckConfigBinder() {

            @Override
            public <C extends Config> C bindConfig(Class<C> configurationInterfaceClass) {
                return deployable.config(configurationInterfaceClass);
            }
        }, new HealthCheckRegistry() {

            @Override
            public void register(HealthChecker healthChecker) {
                deployable.addHealthCheck(healthChecker);
            }

            @Override
            public void unregister(HealthChecker healthChecker) {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        });

        deployable.buildStatusReporter(null).start();
        deployable.buildManageServer().start();

        InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);

        MiruRegistryConfig registryConfig = deployable.config(MiruRegistryConfig.class);

        RowColumnValueStoreProvider<? extends Config, ? extends Exception> rowColumnValueStoreProvider = registryConfig.getRowColumnValueStoreProviderClass()
            .newInstance();
        Config rowColumnValueStoreConfig = deployable.config(rowColumnValueStoreProvider.getConfigurationClass());
        RowColumnValueStoreInitializer<? extends Exception> rowColumnValueStoreInitializer = rowColumnValueStoreProvider.getInitializerClass()
            .getConstructor(rowColumnValueStoreConfig.getClass())
            .newInstance(rowColumnValueStoreConfig);

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());

        MiruClientConfig clientConfig = deployable.config(MiruClientConfig.class);

        MiruRegistryStore registryStore = new MiruRegistryStoreInitializer()
            .initialize(instanceConfig.getClusterName(), rowColumnValueStoreInitializer, mapper);
        MiruClusterRegistry clusterRegistry = new MiruRCVSClusterRegistry(new CurrentTimestamper(),
            registryStore.getHostsRegistry(),
            registryStore.getExpectedTenantsRegistry(),
            registryStore.getExpectedTenantPartitionsRegistry(),
            registryStore.getReplicaRegistry(),
            registryStore.getTopologyRegistry(),
            registryStore.getConfigRegistry(),
            registryStore.getWriterPartitionRegistry(),
            registryConfig.getDefaultNumberOfReplicas(),
            registryConfig.getDefaultTopologyIsStaleAfterMillis());

        MiruReplicaSetDirector replicaSetDirector = new MiruReplicaSetDirector(
            new OrderIdProviderImpl(new ConstantWriterIdProvider(instanceConfig.getInstanceName())),
            clusterRegistry);

        MiruWALInitializer.MiruWAL wal = new MiruWALInitializer().initialize(instanceConfig.getClusterName(), rowColumnValueStoreInitializer, mapper);

        MiruClient miruClient = new MiruClientInitializer().initialize(clientConfig,
            clusterRegistry,
            replicaSetDirector,
            registryStore,
            wal,
            instanceConfig.getInstanceName());

        deployable.addEndpoints(MiruClientEndpoints.class);
        deployable.addInjectables(MiruClient.class, miruClient);
        deployable.addEndpoints(MiruWriterConfigEndpoints.class);

        deployable.buildServer().start();

    }
}
