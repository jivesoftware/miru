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
package com.jivesoftware.os.miru.manage.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.jivesoftware.os.jive.utils.row.column.value.store.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.hbase.HBaseSetOfSortedMapsImplInitializer;
import com.jivesoftware.os.jive.utils.row.column.value.store.hbase.HBaseSetOfSortedMapsImplInitializer.HBaseSetOfSortedMapsConfig;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.MiruRegistryStoreInitializer;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.server.http.jetty.jersey.server.util.Resource;
import com.jivesoftware.os.upena.main.Deployable;
import com.jivesoftware.os.upena.main.InstanceConfig;
import java.io.File;
import java.util.concurrent.TimeUnit;

import static com.jivesoftware.os.miru.manage.deployable.MiruManageInitializer.MiruManageConfig;

public class MiruManageMain {

    public static void main(String[] args) throws Exception {
        new MiruManageMain().run(args);
    }

    public void run(String[] args) throws Exception {

        Deployable deployable = new Deployable(args);
        deployable.buildManageServer().start();

        InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);

        HBaseSetOfSortedMapsConfig hbaseConfig = deployable.config(HBaseSetOfSortedMapsConfig.class);
        SetOfSortedMapsImplInitializer<Exception> setOfSortedMapsInitializer = new HBaseSetOfSortedMapsImplInitializer(hbaseConfig);

        MiruManageConfig manageConfig = deployable.config(MiruManageConfig.class);

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

        MiruManageService miruManageService = new MiruManageInitializer().initialize(manageConfig, clusterRegistry, registryStore, wal);

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());

        Resource sourceTree = new Resource(new File(System.getProperty("user.dir")))
                //.addResourcePath("../../../../../src/main/resources") // fluff?
                .addResourcePath("resources")
                .setContext("/static");

        deployable.addEndpoints(MiruManageEndpoints.class);
        deployable.addInjectables(MiruManageService.class, miruManageService);
        deployable.addResource(sourceTree);

        deployable.buildServer().start();

    }
}
