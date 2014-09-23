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
import com.jivesoftware.os.jive.utils.row.column.value.store.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.jive.utils.row.column.value.store.hbase.HBaseSetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.hbase.HBaseSetOfSortedMapsImplInitializer.HBaseSetOfSortedMapsConfig;
import com.jivesoftware.os.miru.client.MiruClient;
import com.jivesoftware.os.miru.client.MiruClientConfig;
import com.jivesoftware.os.miru.client.MiruClientInitializer;
import com.jivesoftware.os.miru.client.endpoints.MiruClientEndpoints;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.MiruRegistryStoreInitializer;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.upena.main.Deployable;
import com.jivesoftware.os.upena.main.InstanceConfig;
import java.util.concurrent.TimeUnit;

public class MiruWriterMain {

    public static void main(String[] args) throws Exception {
        new MiruWriterMain().run(args);
    }

    public void run(String[] args) throws Exception {

        Deployable deployable = new Deployable(args);
        deployable.buildManageServer().start();

        InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);

        HBaseSetOfSortedMapsConfig hbaseConfig = deployable.config(HBaseSetOfSortedMapsConfig.class);
        SetOfSortedMapsImplInitializer<Exception> setOfSortedMapsInitializer = new HBaseSetOfSortedMapsImplInitializer(hbaseConfig);

        MiruClientConfig clientConfig = deployable.config(MiruClientConfig.class);

        MiruRegistryStore registryStore = new MiruRegistryStoreInitializer().initialize(instanceConfig.getClusterName(), setOfSortedMapsInitializer);
        MiruClusterRegistry clusterRegistry = new MiruRCVSClusterRegistry(new CurrentTimestamper(),
            registryStore.getHostsRegistry(),
            registryStore.getExpectedTenantsRegistry(),
            registryStore.getExpectedTenantPartitionsRegistry(),
            registryStore.getReplicaRegistry(),
            registryStore.getTopologyRegistry(),
            registryStore.getConfigRegistry(),
            3,
            TimeUnit.HOURS.toMillis(1));

        MiruWALInitializer.MiruWAL wal = new MiruWALInitializer().initialize(instanceConfig.getClusterName(), setOfSortedMapsInitializer);

        MiruClient miruClient = new MiruClientInitializer().initialize(clientConfig, clusterRegistry, registryStore, wal, instanceConfig.getInstanceName());

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());

        deployable.addEndpoints(MiruClientEndpoints.class);
        deployable.addInjectables(MiruClient.class, miruClient);

        deployable.buildServer().start();

    }
}
