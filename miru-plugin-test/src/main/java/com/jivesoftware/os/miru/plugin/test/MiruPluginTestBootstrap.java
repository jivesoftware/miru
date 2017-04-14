package com.jivesoftware.os.miru.plugin.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Interners;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.jivesoftware.os.amza.embed.EmbedAmzaServiceInitializer.Lifecycle;
import com.jivesoftware.os.amza.service.EmbeddedClientProvider;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LABStats;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceConfig;
import com.jivesoftware.os.miru.amza.MiruAmzaServiceInitializer;
import com.jivesoftware.os.miru.amza.NoOpClientHealth;
import com.jivesoftware.os.miru.api.HostPortProvider;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruHostSelectiveStrategy;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
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
import com.jivesoftware.os.miru.plugin.MiruInterner;
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
import com.jivesoftware.os.miru.plugin.plugin.IndexCloseCallback;
import com.jivesoftware.os.miru.plugin.plugin.IndexCommitCallback;
import com.jivesoftware.os.miru.plugin.plugin.IndexOpenCallback;
import com.jivesoftware.os.miru.plugin.query.LuceneBackedQueryParser;
import com.jivesoftware.os.miru.plugin.query.MiruQueryParser;
import com.jivesoftware.os.miru.plugin.schema.SingleSchemaProvider;
import com.jivesoftware.os.miru.plugin.solution.MiruRemotePartition;
import com.jivesoftware.os.miru.service.MiruService;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.MiruServiceInitializer;
import com.jivesoftware.os.miru.service.locator.MiruTempDirectoryResourceLocator;
import com.jivesoftware.os.miru.service.partition.PartitionErrorTracker;
import com.jivesoftware.os.miru.service.partition.RCVSSipTrackerFactory;
import com.jivesoftware.os.miru.service.realtime.NoOpRealtimeDelivery;
import com.jivesoftware.os.miru.service.stream.MiruIndexCallbacks;
import com.jivesoftware.os.miru.wal.RCVSWALDirector;
import com.jivesoftware.os.miru.wal.RCVSWALInitializer;
import com.jivesoftware.os.miru.wal.activity.rcvs.RCVSActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.rcvs.RCVSActivityWALWriter;
import com.jivesoftware.os.miru.wal.lookup.MiruWALLookup;
import com.jivesoftware.os.miru.wal.lookup.RCVSWALLookup;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.RCVSReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.rcvs.RCVSReadTrackingWALWriter;
import com.jivesoftware.os.rcvs.inmemory.InMemoryRowColumnValueStoreInitializer;
import com.jivesoftware.os.routing.bird.deployable.Deployable;
import com.jivesoftware.os.routing.bird.health.HealthCheck;
import com.jivesoftware.os.routing.bird.health.api.HealthCheckRegistry;
import com.jivesoftware.os.routing.bird.health.api.HealthChecker;
import com.jivesoftware.os.routing.bird.health.api.HealthFactory;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.BoundedExecutor;
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.merlin.config.BindInterfaceToConfiguration;
import org.merlin.config.Config;

/**
 *
 */
public class MiruPluginTestBootstrap {

    MiruInterner<MiruTermId> termInterner = new MiruInterner<MiruTermId>(true) {
        @Override
        public MiruTermId create(byte[] bytes) {
            return new MiruTermId(bytes);
        }
    };

    public <BM extends IBM, IBM> MiruProvider<MiruService> bootstrap(MiruTenantId tenantId,
        MiruPartitionId partitionId,
        MiruHost miruHost,
        MiruSchema miruSchema,
        MiruBackingStorage desiredStorage,
        final MiruBitmaps<BM, IBM> bitmaps,
        List<MiruPartitionedActivity> includeActivities)
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
        config.setDefaultFailAfterNMillis(TimeUnit.HOURS.toMillis(1));
        config.setPersistentMergeChitCount(10_000);
        config.setTransientMergeChitCount(10_000);

        Logger rootLogger = LogManager.getRootLogger();
        if (rootLogger instanceof org.apache.logging.log4j.core.Logger) {
            LoggerContext context = ((org.apache.logging.log4j.core.Logger) rootLogger).getContext();
            LoggerConfig loggerConfig = context.getConfiguration().getLoggerConfig("");
            loggerConfig.setLevel(Level.WARN);
        }

        ObjectMapper mapper = new ObjectMapper();

        InMemoryRowColumnValueStoreInitializer inMemoryRowColumnValueStoreInitializer = new InMemoryRowColumnValueStoreInitializer();
        RCVSWALInitializer.RCVSWAL wal = new RCVSWALInitializer().initialize("test", inMemoryRowColumnValueStoreInitializer, mapper);
        List<MiruPartitionedActivity> partitionedActivities = Lists.newArrayList();
        partitionedActivities.add(new MiruPartitionedActivityFactory().begin(1, partitionId, tenantId, 0));
        partitionedActivities.addAll(includeActivities);

        RCVSActivityWALWriter activityWALWriter = new RCVSActivityWALWriter(wal.getActivityWAL(), wal.getActivitySipWAL(), null);
        activityWALWriter.write(tenantId, partitionId, partitionedActivities);

        HostPortProvider hostPortProvider = host -> 10_000;

        RCVSActivityWALReader activityWALReader = new RCVSActivityWALReader(hostPortProvider,
            wal.getActivityWAL(),
            wal.getActivitySipWAL(),
            null);
        RCVSReadTrackingWALWriter readTrackingWALWriter = new RCVSReadTrackingWALWriter(wal.getReadTrackingWAL(), wal.getReadTrackingSipWAL());
        RCVSReadTrackingWALReader readTrackingWALReader = new RCVSReadTrackingWALReader(hostPortProvider,
            wal.getReadTrackingWAL(),
            wal.getReadTrackingSipWAL());
        MiruWALLookup walLookup = new RCVSWALLookup(wal.getWALLookupTable());

        MiruClusterRegistry clusterRegistry;

        File amzaDataDir = Files.createTempDir();
        MiruAmzaServiceConfig acrc = BindInterfaceToConfiguration.bindDefault(MiruAmzaServiceConfig.class);
        acrc.setWorkingDirectories(amzaDataDir.getAbsolutePath());
        Deployable deployable = new Deployable(new String[0]);
        Lifecycle amzaLifecycle = new MiruAmzaServiceInitializer().initialize(deployable,
            connectionDescriptor -> new NoOpClientHealth(),
            1,
            "instanceKey",
            "serviceName",
            "datacenter",
            "rack",
            "localhost",
            10000,
            false,
            null,
            acrc,
            false,
            1,
            rowsChanged -> {
            });

        EmbeddedClientProvider amzaClientProvider = new EmbeddedClientProvider(amzaLifecycle.amzaService);
        clusterRegistry = new AmzaClusterRegistry(amzaLifecycle.amzaService,
            amzaClientProvider,
            10_000L,
            new JacksonJsonObjectTypeMarshaller<>(MiruSchema.class, mapper),
            3,
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.HOURS.toMillis(1),
            TimeUnit.DAYS.toMillis(365),
            TimeUnit.DAYS.toMillis(730));

        MiruReplicaSetDirector replicaSetDirector = new MiruReplicaSetDirector(new OrderIdProviderImpl(new ConstantWriterIdProvider(1)), clusterRegistry,
            stream -> stream.descriptor("datacenter", "rack", miruHost), false);
        MiruWALClient<RCVSCursor, RCVSSipCursor> walClient = new RCVSWALDirector(walLookup, activityWALReader, activityWALWriter, readTrackingWALReader,
            readTrackingWALWriter, new MiruRegistryClusterClient(clusterRegistry, replicaSetDirector));
        MiruInboxReadTracker inboxReadTracker = new RCVSInboxReadTracker(walClient);

        MiruRegistryClusterClient clusterClient = new MiruRegistryClusterClient(clusterRegistry, replicaSetDirector);

        clusterRegistry.heartbeat(miruHost);
        replicaSetDirector.electHostsForTenantPartition(tenantId, partitionId, new MiruReplicaSet(ArrayListMultimap.create(), new HashSet<>(), 3, 3));
        clusterRegistry.updateIngress(new MiruIngressUpdate(tenantId, partitionId, new RangeMinMax(), System.currentTimeMillis(), false));


        MiruLifecyle<MiruJustInTimeBackfillerizer> backfillerizerLifecycle = new MiruBackfillerizerInitializer()
            .initialize(Executors.newCachedThreadPool(), config.getReadStreamIdsPropName(), inboxReadTracker, null);

        backfillerizerLifecycle.start();
        final MiruJustInTimeBackfillerizer backfillerizer = backfillerizerLifecycle.getService();

        final MiruTermComposer termComposer = new MiruTermComposer(Charsets.UTF_8, termInterner);

        MiruInterner<MiruIBA> ibaInterner = new MiruInterner<MiruIBA>(true) {
            @Override
            public MiruIBA create(byte[] bytes) {
                return new MiruIBA(bytes);
            }
        };

        MiruInterner<MiruTenantId> tenantInterner = new MiruInterner<MiruTenantId>(true) {
            @Override
            public MiruTenantId create(byte[] bytes) {
                return new MiruTenantId(bytes);
            }
        };

        final MiruActivityInternExtern activityInternExtern = new MiruActivityInternExtern(ibaInterner,
            tenantInterner,
            Interners.<String>newWeakInterner(),
            termComposer);
        final MiruStats miruStats = new MiruStats();

        List<Runnable> scheduledBootstrapExecutorRunnables = new ArrayList<>();
        final ScheduledExecutorService scheduledBootstrapExecutor = new ScheduledThreadPoolExecutor(3) {
            @Override
            public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
                synchronized (scheduledBootstrapExecutorRunnables) {
                    scheduledBootstrapExecutorRunnables.add(command);
                }
                return null;
            }
        };

        List<Runnable> scheduledRebuildExecutorRunnables = new ArrayList<>();
        final ScheduledExecutorService scheduledRebuildExecutor = new ScheduledThreadPoolExecutor(3) {
            @Override
            public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
                synchronized (scheduledRebuildExecutorRunnables) {
                    scheduledRebuildExecutorRunnables.add(command);
                }
                return null;
            }
        };

        List<Runnable> scheduledSipMigrateExecutorRunnables = new ArrayList<>();
        final ScheduledExecutorService scheduledSipMigrateExecutor = new ScheduledThreadPoolExecutor(3) {
            @Override
            public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
                synchronized (scheduledSipMigrateExecutorRunnables) {
                    scheduledSipMigrateExecutorRunnables.add(command);
                }
                return null;
            }
        };


        MiruLifecyle<MiruService> miruServiceLifecyle = new MiruServiceInitializer().initialize(
            config,
            BoundedExecutor.newBoundedExecutor(config.getSolverExecutorThreads(), "solver"),
            BoundedExecutor.newBoundedExecutor(config.getParallelSolversExecutorThreads(), "parallel-solver"),
            BoundedExecutor.newBoundedExecutor(config.getRebuilderThreads(), "rebuild-wal-consumer"),
            BoundedExecutor.newBoundedExecutor(config.getSipIndexerThreads(), "sip-index"),
            BoundedExecutor.newBoundedExecutor(config.getMergeIndexThreads(), "persistent-merge-index"),
            BoundedExecutor.newBoundedExecutor(config.getMergeIndexThreads(), "transient-merge-index"),
            BoundedExecutor.newBoundedExecutor(config.getStreamFactoryExecutorCount(), "stream-factory"),
            miruStats,
            new LABStats(),
            new LABStats(),
            scheduledBootstrapExecutor,
            scheduledRebuildExecutor,
            scheduledSipMigrateExecutor,
            clusterClient,
            miruHost,
            new SingleSchemaProvider(miruSchema),
            walClient,
            new NoOpRealtimeDelivery(miruStats),
            new RCVSSipTrackerFactory(),
            new RCVSSipIndexMarshaller(),
            new MiruTempDirectoryResourceLocator(),
            termComposer,
            activityInternExtern,
            new SingleBitmapsProvider(bitmaps),
            new MiruIndexCallbacks(),
            new PartitionErrorTracker(BindInterfaceToConfiguration.bindDefault(PartitionErrorTracker.PartitionErrorTrackerConfig.class)),
            termInterner,
            new AtomicBoolean(false),
            LABEnvironment.buildLABHeapSchedulerThreadPool(config.getRebuildLabHeapPressureStripes()),
            LABEnvironment.buildLABHeapSchedulerThreadPool(config.getGlobalLabHeapPressureStripes()),
            LABEnvironment.buildLABHeapSchedulerThreadPool(config.getTimeIdLabHeapPressureStripes()),
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool()
        );

        miruServiceLifecyle.start();

        final MiruService miruService = miruServiceLifecyle.getService();

        while (!miruService.checkInfo(tenantId, partitionId, new MiruPartitionCoordInfo(MiruPartitionState.bootstrap, MiruBackingStorage.memory))) {
            System.out.println("Waiting to move out of bootstrap...");
            synchronized (scheduledBootstrapExecutorRunnables) {
                for (Runnable runnable : scheduledBootstrapExecutorRunnables) {
                    runnable.run();
                }
            }
            Thread.sleep(10);
        }

        while (!miruService.checkInfo(tenantId, partitionId, new MiruPartitionCoordInfo(MiruPartitionState.online, MiruBackingStorage.memory))) {
            System.out.println("Waiting to move to online memory...");
            synchronized (scheduledRebuildExecutorRunnables) {
                for (Runnable runnable : scheduledRebuildExecutorRunnables) {
                    runnable.run();
                }
            }
            Thread.sleep(10);
        }

        while (!miruService.checkInfo(tenantId, partitionId, new MiruPartitionCoordInfo(MiruPartitionState.online, desiredStorage))) {
            System.out.println("Waiting to move online " + desiredStorage + "...");
            synchronized (scheduledSipMigrateExecutorRunnables) {
                for (Runnable runnable : scheduledSipMigrateExecutorRunnables) {
                    runnable.run();
                }
            }
            Thread.sleep(10);
        }

        while (!miruService.isAvailable(tenantId, partitionId)) {
            System.out.println("Waiting to become available... ");
            synchronized (scheduledSipMigrateExecutorRunnables) {
                for (Runnable runnable : scheduledSipMigrateExecutorRunnables) {
                    runnable.run();
                }
            }
            Thread.sleep(10);
        }

        return new MiruProvider<MiruService>() {
            @Override
            public MiruService getMiru(MiruTenantId tenantId) {
                return miruService;
            }

            @Override
            public MiruHost getHost() {
                return miruHost;
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
            public TenantAwareHttpClient<String> getCatwalkHttpClient() {
                return null;
            }

            @Override
            public TenantAwareHttpClient<String> getTenantAwareHttpClient(String serviceName, int socketTimeoutMillis) {
                return null;
            }

            @Override
            public Map<MiruHost, MiruHostSelectiveStrategy> getReaderStrategyCache() {
                return null;
            }

            @Override
            public <C extends Config> C getConfig(Class<C> configClass) {
                return BindInterfaceToConfiguration.bindDefault(configClass);
            }

            @Override
            public void addHealthCheck(HealthCheck healthCheck) {
            }

            @Override
            public void addIndexOpenCallback(IndexOpenCallback callback) {
            }

            @Override
            public void addIndexCommitCallback(IndexCommitCallback callback) {
            }

            @Override
            public void addIndexCloseCallback(IndexCloseCallback callback) {
            }

            @Override
            public void removeIndexOpenCallback(IndexOpenCallback callback) {
            }

            @Override
            public void removeIndexCommitCallback(IndexCommitCallback callback) {
            }

            @Override
            public void removeIndexCloseCallback(IndexCloseCallback callback) {
            }

            @Override
            public ExecutorService allocateThreadPool(String name, int maxThreads) {
                return BoundedExecutor.newBoundedExecutor(maxThreads, name);
            }
        };
    }

}
