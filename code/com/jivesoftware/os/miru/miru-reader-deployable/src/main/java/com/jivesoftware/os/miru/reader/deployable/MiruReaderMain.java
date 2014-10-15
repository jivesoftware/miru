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
import com.jivesoftware.os.jive.utils.health.api.HealthCheckConfigBinder;
import com.jivesoftware.os.jive.utils.health.api.HealthCheckRegistry;
import com.jivesoftware.os.jive.utils.health.api.HealthChecker;
import com.jivesoftware.os.jive.utils.health.api.HealthFactory;
import com.jivesoftware.os.jive.utils.health.checkers.GCLoadHealthChecker;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.api.MiruWriter;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.MiruRegistryStoreInitializer;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.plugin.Miru;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.bitmap.SingleBitmapsProvider;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruJustInTimeBackfillerizer;
import com.jivesoftware.os.miru.plugin.plugin.MiruEndpointInjectable;
import com.jivesoftware.os.miru.plugin.plugin.MiruPlugin;
import com.jivesoftware.os.miru.service.MiruBackfillerizerInitializer;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.MiruServiceInitializer;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.service.endpoint.MiruReaderEndpoints;
import com.jivesoftware.os.miru.service.endpoint.MiruWriterEndpoints;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocatorProvider;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocatorProviderInitializer;
import com.jivesoftware.os.miru.service.schema.RegistrySchemaProvider;
import com.jivesoftware.os.miru.service.writer.MiruWriterImpl;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.rcvs.api.SetOfSortedMapsImplInitializer;
import com.jivesoftware.os.rcvs.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.rcvs.hbase.HBaseSetOfSortedMapsImplInitializer;
import com.jivesoftware.os.rcvs.hbase.HBaseSetOfSortedMapsImplInitializer.HBaseSetOfSortedMapsConfig;
import com.jivesoftware.os.upena.main.Deployable;
import com.jivesoftware.os.upena.main.InstanceConfig;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.merlin.config.Config;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

public class MiruReaderMain {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static void main(String[] args) throws Exception {
        new MiruReaderMain().run(args);
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
        deployable.addHealthCheck(new GCLoadHealthChecker(deployable.config(GCLoadHealthChecker.GCLoadHealthCheckerConfig.class)));
        deployable.buildManageServer().start();

        InstanceConfig instanceConfig = deployable.config(InstanceConfig.class);
        MiruHost miruHost = new MiruHost(instanceConfig.getHost(), instanceConfig.getMainPort());

        HBaseSetOfSortedMapsConfig hbaseConfig = deployable.config(HBaseSetOfSortedMapsConfig.class);
        SetOfSortedMapsImplInitializer<Exception> setOfSortedMapsInitializer = new HBaseSetOfSortedMapsImplInitializer(hbaseConfig);

        MiruServiceConfig miruServiceConfig = deployable.config(MiruServiceConfig.class);

        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider()
            .createHttpClientFactory(Collections.<HttpClientConfiguration>emptyList());

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new GuavaModule());

        MiruRegistryStore registryStore = new MiruRegistryStoreInitializer().initialize(instanceConfig.getClusterName(),
                setOfSortedMapsInitializer, mapper);
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
        RegistrySchemaProvider registrySchemaProvider = new RegistrySchemaProvider(registryStore.getSchemaRegistry(), 10_000);

        MiruLifecyle<MiruService> miruServiceLifecyle = new MiruServiceInitializer().initialize(miruServiceConfig,
            registryStore,
            clusterRegistry,
            miruHost,
            registrySchemaProvider, //TODO configure
            wal,
            httpClientFactory,
            miruResourceLocatorProviderLifecyle.getService(),
            internExtern,
            new SingleBitmapsProvider<>(bitmaps));

        miruServiceLifecyle.start();
        final MiruService miruService = miruServiceLifecyle.getService();
        MiruWriter miruWriter = new MiruWriterImpl(miruService);

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

        for (String pluginPackage : miruServiceConfig.getPluginPackages().split(",")) {
            Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(pluginPackage.trim()))
                .setScanners(new SubTypesScanner(), new TypesScanner()));
            Set<Class<? extends MiruPlugin>> pluginTypes = reflections.getSubTypesOf(MiruPlugin.class);
            for (Class<? extends MiruPlugin> pluginType : pluginTypes) {
                LOG.info("Loading plugin {}", pluginType.getSimpleName());
                add(miruProvider, deployable, pluginType.newInstance());
            }
        }

        deployable.addEndpoints(MiruReaderConfigEndpoints.class);
        deployable.addInjectables(MiruClusterRegistry.class, clusterRegistry);
        deployable.addInjectables(RegistrySchemaProvider.class, registrySchemaProvider);

        deployable.buildServer().start();

    }

    private <E, I> void add(MiruProvider<? extends Miru> miruProvider, Deployable deployable, MiruPlugin<E, I> plugin) {
        Class<E> endpointsClass = plugin.getEndpointsClass();
        deployable.addEndpoints(endpointsClass);
        Collection<MiruEndpointInjectable<I>> injectables = plugin.getInjectables(miruProvider);
        for (MiruEndpointInjectable<?> miruEndpointInjectable : injectables) {
            deployable.addInjectables(miruEndpointInjectable.getInjectableClass(), miruEndpointInjectable.getInjectable());
        }
    }
}
