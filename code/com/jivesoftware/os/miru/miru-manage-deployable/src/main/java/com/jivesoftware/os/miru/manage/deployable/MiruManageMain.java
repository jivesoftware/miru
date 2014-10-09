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
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.MiruRegistryStoreInitializer;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.manage.deployable.MiruManageInitializer.MiruManageConfig;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.rcvs.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.rcvs.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.rcvs.hbase.HBaseSetOfSortedMapsImplInitializer;
import com.jivesoftware.os.rcvs.hbase.HBaseSetOfSortedMapsImplInitializer.HBaseSetOfSortedMapsConfig;
import com.jivesoftware.os.server.http.jetty.jersey.server.util.Resource;
import com.jivesoftware.os.upena.main.Deployable;
import com.jivesoftware.os.upena.main.InstanceConfig;
import java.io.File;
import java.util.concurrent.TimeUnit;

public class MiruManageMain {

    public static void main(String[] args) throws Exception {
        new MiruManageMain().run(args);
    }

    public void run(String[] args) throws Exception {

        Deployable deployable = new Deployable(args);
        deployable.buildStatusReporter(null).start();
        deployable.buildManageServer().start();

        InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);

        HBaseSetOfSortedMapsConfig hbaseConfig = deployable.config(HBaseSetOfSortedMapsConfig.class);
        SetOfSortedMapsImplInitializer<Exception> setOfSortedMapsInitializer = new HBaseSetOfSortedMapsImplInitializer(hbaseConfig);

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());

        MiruManageConfig manageConfig = deployable.config(MiruManageConfig.class);

        MiruRegistryStore registryStore = new MiruRegistryStoreInitializer().initialize(instanceConfig.getClusterName(), setOfSortedMapsInitializer, mapper);
        MiruClusterRegistry clusterRegistry = new MiruRCVSClusterRegistry(new CurrentTimestamper(),
                registryStore.getHostsRegistry(),
                registryStore.getExpectedTenantsRegistry(),
                registryStore.getExpectedTenantPartitionsRegistry(),
                registryStore.getReplicaRegistry(),
                registryStore.getTopologyRegistry(),
                registryStore.getConfigRegistry(),
                3,
                TimeUnit.HOURS.toMillis(1));

        MiruWALInitializer.MiruWAL wal = new MiruWALInitializer().initialize(instanceConfig.getClusterName(), setOfSortedMapsInitializer, mapper);

        MiruManageService miruManageService = new MiruManageInitializer().initialize(manageConfig, clusterRegistry, registryStore, wal);

        File staticResourceDir = new File(System.getProperty("user.dir"));
        System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
        Resource sourceTree = new Resource(staticResourceDir)
                //.addResourcePath("../../../../../src/main/resources") // fluff?
                .addResourcePath(manageConfig.getPathToStaticResources())
                .setContext("/static");

        deployable.addEndpoints(MiruManageEndpoints.class);
        deployable.addInjectables(MiruManageService.class, miruManageService);
        deployable.addResource(sourceTree);

        deployable.buildServer().start();

    }
}
