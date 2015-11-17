package com.jivesoftware.os.miru.plugin.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Interners;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.client.AmzaClientProvider;
import com.jivesoftware.os.amza.service.AmzaService;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceConfig;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceInitializer;
import com.jivesoftware.os.miru.api.HostPortProvider;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.marshall.JacksonJsonObjectTypeMarshaller;
import com.jivesoftware.os.miru.api.topology.MiruIngressUpdate;
import com.jivesoftware.os.miru.api.topology.RangeMinMax;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.api.wal.RCVSCursor;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryClusterClient;
import com.jivesoftware.os.miru.cluster.MiruReplicaSet;
import com.jivesoftware.os.miru.cluster.MiruReplicaSetDirector;
import com.jivesoftware.os.miru.cluster.amza.AmzaClusterRegistry;
import com.jivesoftware.os.miru.plugin.MiruProvider;
import com.jivesoftware.os.miru.plugin.backfill.MiruInboxReadTracker;
import com.jivesoftware.os.miru.plugin.backfill.MiruJustInTimeBackfillerizer;
import com.jivesoftware.os.miru.plugin.backfill.RCVSInboxReadTracker;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.SingleBitmapsProvider;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruBackfillerizerInitializer;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.marshaller.RCVSSipIndexMarshaller;
import com.jivesoftware.os.miru.plugin.query.LuceneBackedQueryParser;
import com.jivesoftware.os.miru.plugin.query.MiruQueryParser;
import com.jivesoftware.os.miru.plugin.schema.SingleSchemaProvider;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.MiruServiceInitializer;
import com.jivesoftware.os.miru.service.locator.MiruTempDirectoryResourceLocator;
import com.jivesoftware.os.miru.service.partition.RCVSSipTrackerFactory;
import com.jivesoftware.os.miru.wal.MiruWALDirector;
import com.jivesoftware.os.miru.wal.RCVSWALInitializer;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.rcvs.RCVSActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.rcvs.RCVSActivityWALWriter;
import com.jivesoftware.os.miru.wal.lookup.MiruWALLookup;
import com.jivesoftware.os.miru.wal.lookup.RCVSWALLookup;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALWriter;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.RCVSReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.RCVSReadTrackingWALWriter;
import com.jivesoftware.os.rcvs.inmemory.InMemoryRowColumnValueStoreInitializer;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckRegistry;
import com.jivesoftware.os.routing.bird.health.api.HealthChecker;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.http.client.ConnectionDescriptorSelectiveStrategy;
import com.jivesoftware.os.routing.bird.http.client.HttpClient;
import com.jivesoftware.os.routing.bird.http.client.HttpClientException;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.merlin.config.BindInterfaceToConfiguration;
import org.merlin.config.Config;
import org.testng.Assert;

/**
 *
 */
public class MiruPluginTestBootstrap {

    public <BM> MiruProvider<MiruService> bootstrap(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        MiruHost miruHost,
        MiruSchema miruSchema,
        MiruBackingStorage desiredStorage,
        final MiruBitmaps<BM> bitmaps,
        List<MiruPartitionedActivity> partitionedActivities)
        throws Exception {

        HealthFactory.initialize(
            BindInterfaceToConfiguration::bindDefault,
            new HealthCheckRegistry() {
                @Override
                public void register(HealthChecker healthChecker) {
                }

                @Override
                public void unregister(HealthChecker healthChecker) {
                }
            });

        MiruServiceConfig config = BindInterfaceToConfiguration.bindDefault(MiruServiceConfig.class);
        config.setDefaultStorage(desiredStorage.name());
        config.setDefaultFailAfterNMillis(TimeUnit.HOURS.toMillis(1));
        config.setMergeChitCount(10_000);

        Logger rootLogger = LogManager.getRootLogger();
        if (rootLogger instanceof org.apache.logging.log4j.core.Logger) {
            LoggerContext context = ((org.apache.logging.log4j.core.Logger) rootLogger).getContext();
            LoggerConfig loggerConfig = context.getConfiguration().getLoggerConfig("");
            loggerConfig.setLevel(Level.WARN);
        }

        ObjectMapper mapper = new ObjectMapper();

        InMemoryRowColumnValueStoreInitializer inMemoryRowColumnValueStoreInitializer = new InMemoryRowColumnValueStoreInitializer();
        RCVSWALInitializer.RCVSWAL wal = new RCVSWALInitializer().initialize("test", inMemoryRowColumnValueStoreInitializer, mapper);
        if (!partitionedActivities.isEmpty()) {
            MiruActivityWALWriter activityWALWriter = new RCVSActivityWALWriter(wal.getActivityWAL(), wal.getActivitySipWAL());
            activityWALWriter.write(tenantId, partitionId, partitionedActivities);
        }

        HostPortProvider hostPortProvider = host -> 10_000;

        MiruActivityWALWriter activityWALWriter = new RCVSActivityWALWriter(wal.getActivityWAL(), wal.getActivitySipWAL());
        MiruActivityWALReader<RCVSCursor, RCVSSipCursor> activityWALReader = new RCVSActivityWALReader(hostPortProvider,
            wal.getActivityWAL(),
            wal.getActivitySipWAL());
        MiruReadTrackingWALWriter readTrackingWALWriter = new RCVSReadTrackingWALWriter(wal.getReadTrackingWAL(), wal.getReadTrackingSipWAL());
        MiruReadTrackingWALReader<RCVSCursor, RCVSSipCursor> readTrackingWALReader = new RCVSReadTrackingWALReader(hostPortProvider,
            wal.getReadTrackingWAL(),
            wal.getReadTrackingSipWAL());
        MiruWALLookup walLookup = new RCVSWALLookup(wal.getWALLookupTable());

        MiruClusterRegistry clusterRegistry;

        File amzaDataDir = Files.createTempDir();
        File amzaIndexDir = Files.createTempDir();
        MiruAmzaServiceConfig acrc = BindInterfaceToConfiguration.bindDefault(MiruAmzaServiceConfig.class);
        acrc.setWorkingDirectories(amzaDataDir.getAbsolutePath());
        acrc.setIndexDirectories(amzaIndexDir.getAbsolutePath());
        Deployable deployable = new Deployable(new String[0]);
        AmzaService amzaService = new MiruAmzaServiceInitializer().initialize(deployable, 1, "instanceKey", "serviceName", "localhost", 10000, null, acrc,
            rowsChanged -> {
            });

        AmzaClientProvider amzaClientProvider = new AmzaClientProvider(amzaService);
        clusterRegistry = new AmzaClusterRegistry(amzaService,
            amzaClientProvider,
            0,
            10_000L,
            new JacksonJsonObjectTypeMarshaller<>(MiruSchema.class, mapper),
            3,
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.DAYS.toMillis(365),
            0);

        MiruReplicaSetDirector replicaSetDirector = new MiruReplicaSetDirector(new OrderIdProviderImpl(new ConstantWriterIdProvider(1)), clusterRegistry);
        MiruWALClient<RCVSCursor, RCVSSipCursor> walClient = new MiruWALDirector<>(walLookup, activityWALReader, activityWALWriter, readTrackingWALReader,
            readTrackingWALWriter, new MiruRegistryClusterClient(clusterRegistry, replicaSetDirector));
        MiruInboxReadTracker inboxReadTracker = new RCVSInboxReadTracker(walClient);

        MiruRegistryClusterClient clusterClient = new MiruRegistryClusterClient(clusterRegistry, replicaSetDirector);

        clusterRegistry.heartbeat(miruHost);
        replicaSetDirector.electHostsForTenantPartition(tenantId, partitionId, new MiruReplicaSet(ArrayListMultimap.create(), new HashSet<>(), 3));
        clusterRegistry.updateIngress(new MiruIngressUpdate(tenantId, partitionId, new RangeMinMax(), System.currentTimeMillis(), false));

        MiruLifecyle<MiruJustInTimeBackfillerizer> backfillerizerLifecycle = new MiruBackfillerizerInitializer()
            .initialize(config.getReadStreamIdsPropName(), miruHost, inboxReadTracker);

        backfillerizerLifecycle.start();
        final MiruJustInTimeBackfillerizer backfillerizer = backfillerizerLifecycle.getService();

        final MiruTermComposer termComposer = new MiruTermComposer(Charsets.UTF_8);
        final MiruActivityInternExtern activityInternExtern = new MiruActivityInternExtern(Interners.<MiruIBA>newWeakInterner(),
            Interners.<MiruTermId>newWeakInterner(),
            Interners.<MiruTenantId>newWeakInterner(),
            Interners.<String>newWeakInterner(),
            termComposer);
        final MiruStats miruStats = new MiruStats();

        MiruLifecyle<MiruService> miruServiceLifecyle = new MiruServiceInitializer().initialize(config, miruStats,
            clusterClient,
            miruHost,
            new SingleSchemaProvider(miruSchema),
            walClient,
            new RCVSSipTrackerFactory(),
            new RCVSSipIndexMarshaller(),
            new MiruTempDirectoryResourceLocator(),
            termComposer,
            activityInternExtern,
            new SingleBitmapsProvider<>(bitmaps));

        miruServiceLifecyle.start();
        final MiruService miruService = miruServiceLifecyle.getService();

        long t = System.currentTimeMillis();
        int maxSecondsToComeOnline = 10 + partitionedActivities.size() / 1_000; // suppose 1K activities/sec
        while (!miruService.checkInfo(tenantId, partitionId, new MiruPartitionCoordInfo(MiruPartitionState.online, desiredStorage))) {
            Thread.sleep(10);
            if (System.currentTimeMillis() - t > TimeUnit.SECONDS.toMillis(maxSecondsToComeOnline)) {
                Assert.fail("Partition failed to come online");
            }
        }

        return new MiruProvider<MiruService>() {
            @Override
            public MiruService getMiru(MiruTenantId tenantId) {
                return miruService;
            }

            @Override
            public MiruActivityInternExtern getActivityInternExtern(MiruTenantId tenantId) {
                return activityInternExtern;
            }

            @Override
            public MiruJustInTimeBackfillerizer getBackfillerizer(MiruTenantId tenantId) {
                return backfillerizer;
            }

            @Override
            public MiruTermComposer getTermComposer() {
                return termComposer;
            }

            @Override
            public MiruQueryParser getQueryParser(String defaultField) {
                return new LuceneBackedQueryParser(defaultField);
            }

            @Override
            public MiruStats getStats() {
                return miruStats;
            }

            @Override
            public <R extends MiruRemotePartition<?, ?, ?>> R getRemotePartition(Class<R> remotePartitionClass) {
                return null;
            }

            @Override
            public TenantAwareHttpClient<String> getReaderHttpClient() {
                return null;
            }

            @Override
            public Map<MiruHost, ConnectionDescriptorSelectiveStrategy> getReaderStrategyCache() {
                return null;
            }

            @Override
            public <C extends Config> C getConfig(Class<C> configClass) {
                return BindInterfaceToConfiguration.bindDefault(configClass);
            }
        };
    }
}
