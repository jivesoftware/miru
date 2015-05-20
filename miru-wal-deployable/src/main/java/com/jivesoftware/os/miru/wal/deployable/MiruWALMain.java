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
package com.jivesoftware.os.miru.wal.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.amza.shared.PrimaryIndexDescriptor;
import com.jivesoftware.os.amza.shared.RegionProperties;
import com.jivesoftware.os.amza.shared.WALStorageDescriptor;
import com.jivesoftware.os.jive.utils.health.api.HealthCheckRegistry;
import com.jivesoftware.os.jive.utils.health.api.HealthChecker;
import com.jivesoftware.os.jive.utils.health.api.HealthFactory;
import com.jivesoftware.os.jive.utils.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.jive.utils.health.checkers.ServiceStartupHealthCheck;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceConfig;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceInitializer;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.wal.AmzaCursor;
import com.jivesoftware.os.miru.api.wal.AmzaSipCursor;
import com.jivesoftware.os.miru.api.wal.RCVSCursor;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.cluster.client.MiruClusterClientInitializer;
import com.jivesoftware.os.miru.logappender.MiruLogAppender;
import com.jivesoftware.os.miru.logappender.MiruLogAppenderInitializer;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSampler;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer;
import com.jivesoftware.os.miru.metric.sampler.MiruMetricSamplerInitializer.MiruMetricSamplerConfig;
import com.jivesoftware.os.miru.ui.MiruSoyRenderer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer;
import com.jivesoftware.os.miru.ui.MiruSoyRendererInitializer.MiruSoyRendererConfig;
import com.jivesoftware.os.miru.wal.AmzaWALUtil;
import com.jivesoftware.os.miru.wal.MiruWALDirector;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.miru.wal.activity.ForkingActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.amza.AmzaActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.amza.AmzaActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.rcvs.RCVSActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.rcvs.RCVSActivityWALWriter;
import com.jivesoftware.os.miru.wal.deployable.endpoints.AmzaWALEndpoints;
import com.jivesoftware.os.miru.wal.deployable.endpoints.RCVSWALEndpoints;
import com.jivesoftware.os.miru.wal.lookup.AmzaWALLookup;
import com.jivesoftware.os.miru.wal.lookup.ForkingWALLookup;
import com.jivesoftware.os.miru.wal.lookup.RCVSWALLookup;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALWriter;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.RCVSReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.RCVSReadTrackingWALWriter;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreInitializer;
import com.jivesoftware.os.rcvs.api.RowColumnValueStoreProvider;
import com.jivesoftware.os.server.http.jetty.jersey.endpoints.base.HasUI;
import com.jivesoftware.os.server.http.jetty.jersey.server.util.Resource;
import com.jivesoftware.os.upena.main.Deployable;
import com.jivesoftware.os.upena.main.InstanceConfig;
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.upena.tenant.routing.http.client.TenantRoutingHttpClientInitializer;
import java.io.File;
import java.util.Arrays;
import org.merlin.config.defaults.StringDefault;

public class MiruWALMain {

    public static void main(String[] args) throws Exception {
        new MiruWALMain().run(args);
    }

    public interface WALAmzaServiceConfig extends MiruAmzaServiceConfig {

        @StringDefault("./var/amza/wal/data/")
        @Override
        String getWorkingDirectories();

        @StringDefault("./var/amza/wal/index/")
        @Override
        String getIndexDirectories();

    }

    public void run(String[] args) throws Exception {
        ServiceStartupHealthCheck serviceStartupHealthCheck = new ServiceStartupHealthCheck();
        try {
            final Deployable deployable = new Deployable(args);
            HealthFactory.initialize(deployable::config,
                new HealthCheckRegistry() {

                    @Override
                    public void register(HealthChecker healthChecker) {
                        deployable.addHealthCheck(healthChecker);
                    }

                    @Override
                    public void unregister(HealthChecker healthChecker) {
                        throw new UnsupportedOperationException("Not supported yet.");
                    }
                });
            deployable.addErrorHealthChecks();
            deployable.addManageInjectables(HasUI.class, new HasUI(Arrays.asList(
                new HasUI.UI("Reset Errors", "manage", "/manage/resetErrors"),
                new HasUI.UI("Tail", "manage", "/manage/tail"),
                new HasUI.UI("Thead Dump", "manage", "/manage/threadDump"),
                new HasUI.UI("Health", "manage", "/manage/ui"),
                new HasUI.UI("Miru-WAL", "main", "/miru/wal"),
                new HasUI.UI("Miru-WAL-Amza", "main", "/amza"))));

            deployable.buildStatusReporter(null).start();
            deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
            deployable.addHealthCheck(serviceStartupHealthCheck);
            deployable.buildManageServer().start();

            InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);

            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            mapper.registerModule(new GuavaModule());

            MiruLogAppenderInitializer.MiruLogAppenderConfig miruLogAppenderConfig = deployable.config(MiruLogAppenderInitializer.MiruLogAppenderConfig.class);
            MiruLogAppender miruLogAppender = new MiruLogAppenderInitializer().initialize(null, //TODO datacenter
                instanceConfig.getClusterName(),
                instanceConfig.getHost(),
                instanceConfig.getServiceName(),
                String.valueOf(instanceConfig.getInstanceName()),
                instanceConfig.getVersion(),
                miruLogAppenderConfig);
            miruLogAppender.install();

            MiruMetricSamplerConfig metricSamplerConfig = deployable.config(MiruMetricSamplerConfig.class);
            MiruMetricSampler sampler = new MiruMetricSamplerInitializer().initialize(null, //TODO datacenter
                instanceConfig.getClusterName(),
                instanceConfig.getHost(),
                instanceConfig.getServiceName(),
                String.valueOf(instanceConfig.getInstanceName()),
                instanceConfig.getVersion(),
                metricSamplerConfig);
            sampler.start();

            RCVSWALConfig rcvsWALConfig = deployable.config(RCVSWALConfig.class);
            com.jivesoftware.os.miru.api.wal.MiruWALConfig walConfig = deployable.config(com.jivesoftware.os.miru.api.wal.MiruWALConfig.class);

            RowColumnValueStoreProvider rowColumnValueStoreProvider = rcvsWALConfig.getRowColumnValueStoreProviderClass()
                .newInstance();
            @SuppressWarnings("unchecked")
            RowColumnValueStoreInitializer<? extends Exception> rowColumnValueStoreInitializer = rowColumnValueStoreProvider
                .create(deployable.config(rowColumnValueStoreProvider.getConfigurationClass()));

            MiruStats miruStats = new MiruStats();

            MiruWALInitializer.MiruWAL wal = new MiruWALInitializer().initialize(instanceConfig.getClusterName(), rowColumnValueStoreInitializer, mapper);

            WALAmzaServiceConfig amzaServiceConfig = deployable.config(WALAmzaServiceConfig.class);
            AmzaService amzaService = new MiruAmzaServiceInitializer().initialize(deployable,
                instanceConfig.getInstanceName(),
                instanceConfig.getHost(),
                instanceConfig.getMainPort(),
                "miru-wal-" + instanceConfig.getClusterName(),
                amzaServiceConfig,
                changes -> {
                });
            WALStorageDescriptor storageDescriptor = new WALStorageDescriptor(new PrimaryIndexDescriptor("berkeleydb", 0, false, null),
                null, 1000, 1000);
            AmzaWALUtil amzaWALUtil = new AmzaWALUtil(amzaService,
                new RegionProperties(storageDescriptor, amzaServiceConfig.getReplicationFactor(), amzaServiceConfig.getTakeFromFactor(), false));

            MiruReadTrackingWALWriter readTrackingWALWriter = new RCVSReadTrackingWALWriter(wal.getReadTrackingWAL(), wal.getReadTrackingSipWAL());
            MiruReadTrackingWALReader readTrackingWALReader = new RCVSReadTrackingWALReader(wal.getReadTrackingWAL(), wal.getReadTrackingSipWAL());

            TenantRoutingHttpClientInitializer<String> tenantRoutingHttpClientInitializer = new TenantRoutingHttpClientInitializer<>();
            TenantAwareHttpClient<String> manageHttpClient = tenantRoutingHttpClientInitializer.initialize(deployable
                .getTenantRoutingProvider()
                .getConnections("miru-manage", "main")); // TODO expose to conf

            MiruClusterClient clusterClient = new MiruClusterClientInitializer().initialize(miruStats, "", manageHttpClient, mapper);

            MiruActivityWALReader<?, ?> activityWALReader;
            MiruWALDirector<RCVSCursor, RCVSSipCursor> rcvsWALDirector = null;
            MiruWALDirector<AmzaCursor, AmzaSipCursor> amzaWALDirector = null;
            MiruWALDirector<?, ?> miruWALDirector;
            Class<?> walEndpointsClass;

            if (walConfig.getActivityWALType().equals("rcvs")) {
                RCVSActivityWALWriter rcvsActivityWALWriter = new RCVSActivityWALWriter(wal.getActivityWAL(), wal.getActivitySipWAL());
                RCVSActivityWALReader rcvsActivityWALReader = new RCVSActivityWALReader(wal.getActivityWAL(), wal.getActivitySipWAL());
                RCVSWALLookup rcvsWALLookup = new RCVSWALLookup(wal.getActivityLookupTable());
                rcvsWALDirector = new MiruWALDirector<>(rcvsWALLookup,
                    rcvsActivityWALReader,
                    rcvsActivityWALWriter,
                    readTrackingWALReader,
                    readTrackingWALWriter,
                    clusterClient);

                activityWALReader = rcvsActivityWALReader;
                miruWALDirector = rcvsWALDirector;
                walEndpointsClass = RCVSWALEndpoints.class;
            } else if (walConfig.getActivityWALType().equals("amza")) {
                AmzaActivityWALWriter amzaActivityWALWriter = new AmzaActivityWALWriter(amzaWALUtil, 3, 1, mapper); //TODO ringSize?
                AmzaActivityWALReader amzaActivityWALReader = new AmzaActivityWALReader(amzaWALUtil, mapper);
                AmzaWALLookup amzaWALLookup = new AmzaWALLookup(amzaWALUtil, 3);
                amzaWALDirector = new MiruWALDirector<>(amzaWALLookup,
                    amzaActivityWALReader,
                    amzaActivityWALWriter,
                    readTrackingWALReader,
                    readTrackingWALWriter,
                    clusterClient);

                activityWALReader = amzaActivityWALReader;
                miruWALDirector = amzaWALDirector;
                walEndpointsClass = AmzaWALEndpoints.class;
            } else if (walConfig.getActivityWALType().equals("rcvs_amza")) {
                RCVSActivityWALWriter rcvsActivityWALWriter = new RCVSActivityWALWriter(wal.getActivityWAL(), wal.getActivitySipWAL());
                AmzaActivityWALWriter amzaActivityWALWriter = new AmzaActivityWALWriter(amzaWALUtil, 3, 1, mapper); //TODO ringSize?
                ForkingActivityWALWriter forkingActivityWALWriter = new ForkingActivityWALWriter(rcvsActivityWALWriter, amzaActivityWALWriter);
                RCVSActivityWALReader rcvsActivityWALReader = new RCVSActivityWALReader(wal.getActivityWAL(), wal.getActivitySipWAL());
                RCVSWALLookup rcvsWALLookup = new RCVSWALLookup(wal.getActivityLookupTable());
                AmzaWALLookup amzaWALLookup = new AmzaWALLookup(amzaWALUtil, 3);
                ForkingWALLookup forkingWALLookup = new ForkingWALLookup(rcvsWALLookup, amzaWALLookup);
                rcvsWALDirector = new MiruWALDirector<>(forkingWALLookup,
                    rcvsActivityWALReader,
                    forkingActivityWALWriter,
                    readTrackingWALReader,
                    readTrackingWALWriter,
                    clusterClient);

                activityWALReader = rcvsActivityWALReader;
                miruWALDirector = rcvsWALDirector;
                walEndpointsClass = RCVSWALEndpoints.class;
            } else if (walConfig.getActivityWALType().equals("amza_rcvs")) {
                RCVSActivityWALWriter rcvsActivityWALWriter = new RCVSActivityWALWriter(wal.getActivityWAL(), wal.getActivitySipWAL());
                AmzaActivityWALWriter amzaActivityWALWriter = new AmzaActivityWALWriter(amzaWALUtil, 3, 1, mapper); //TODO ringSize?
                ForkingActivityWALWriter forkingActivityWALWriter = new ForkingActivityWALWriter(amzaActivityWALWriter, rcvsActivityWALWriter);
                AmzaActivityWALReader amzaActivityWALReader = new AmzaActivityWALReader(amzaWALUtil, mapper);
                RCVSWALLookup rcvsWALLookup = new RCVSWALLookup(wal.getActivityLookupTable());
                AmzaWALLookup amzaWALLookup = new AmzaWALLookup(amzaWALUtil, 3);
                ForkingWALLookup forkingWALLookup = new ForkingWALLookup(amzaWALLookup, rcvsWALLookup);
                amzaWALDirector = new MiruWALDirector<>(forkingWALLookup,
                    amzaActivityWALReader,
                    forkingActivityWALWriter,
                    readTrackingWALReader,
                    readTrackingWALWriter,
                    clusterClient);

                activityWALReader = amzaActivityWALReader;
                miruWALDirector = amzaWALDirector;
                walEndpointsClass = AmzaWALEndpoints.class;
            } else {
                throw new IllegalStateException("Invalid activity WAL type: " + walConfig.getActivityWALType());
            }

            MiruSoyRendererConfig rendererConfig = deployable.config(MiruSoyRendererConfig.class);

            File staticResourceDir = new File(System.getProperty("user.dir"));
            System.out.println("Static resources rooted at " + staticResourceDir.getAbsolutePath());
            Resource sourceTree = new Resource(staticResourceDir)
                //.addResourcePath("../../../../../src/main/resources") // fluff?
                .addResourcePath(rendererConfig.getPathToStaticResources())
                .setContext("/static");

            MiruSoyRenderer renderer = new MiruSoyRendererInitializer().initialize(rendererConfig);

            MiruWALUIService miruWALUIService = new MiruWriterUIServiceInitializer()
                .initialize(renderer, rcvsWALDirector, amzaWALDirector, activityWALReader, miruStats);

            deployable.addEndpoints(MiruWALEndpoints.class);
            deployable.addInjectables(MiruWALUIService.class, miruWALUIService);
            deployable.addInjectables(MiruWALDirector.class, miruWALDirector);

            deployable.addEndpoints(walEndpointsClass);
            deployable.addInjectables(MiruStats.class, miruStats);

            deployable.addResource(sourceTree);
            deployable.buildServer().start();
            serviceStartupHealthCheck.success();
        } catch (Throwable t) {
            serviceStartupHealthCheck.info("Encountered the following failure during startup.", t);
        }
    }
}
