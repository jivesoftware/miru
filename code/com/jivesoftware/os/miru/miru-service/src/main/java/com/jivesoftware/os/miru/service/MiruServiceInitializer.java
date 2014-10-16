package com.jivesoftware.os.miru.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.DirectByteBufferFactory;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.cluster.MiruActivityLookupTable;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSActivityLookupTable;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsProvider;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocatorProvider;
import com.jivesoftware.os.miru.service.partition.MiruClusterPartitionDirector;
import com.jivesoftware.os.miru.service.partition.MiruExpectedTenants;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartitionComparison;
import com.jivesoftware.os.miru.service.partition.MiruLocalPartitionFactory;
import com.jivesoftware.os.miru.service.partition.MiruPartitionEventHandler;
import com.jivesoftware.os.miru.service.partition.MiruPartitionInfoProvider;
import com.jivesoftware.os.miru.service.partition.MiruRemotePartitionFactory;
import com.jivesoftware.os.miru.service.partition.MiruTenantTopologyFactory;
import com.jivesoftware.os.miru.service.partition.cluster.CachedClusterPartitionInfoProvider;
import com.jivesoftware.os.miru.service.partition.cluster.MiruClusterExpectedTenants;
import com.jivesoftware.os.miru.service.solver.MiruLowestLatencySolver;
import com.jivesoftware.os.miru.service.solver.MiruSolver;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.wal.MiruWALInitializer.MiruWAL;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReaderImpl;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.MiruWriteToActivityAndSipWAL;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import com.jivesoftware.os.rcvs.api.timestamper.CurrentTimestamper;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MiruServiceInitializer {

    public MiruLifecyle<MiruService> initialize(final MiruServiceConfig config,
            MiruRegistryStore registryStore,
            MiruClusterRegistry clusterRegistry,
            MiruHost miruHost,
            MiruSchemaProvider schemaProvider,
            MiruWAL wal,
            HttpClientFactory httpClientFactory,
            MiruResourceLocatorProvider resourceLocatorProvider,
            MiruActivityInternExtern internExtern,
            MiruBitmapsProvider bitmapsProvider) throws IOException {

        final ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();

        // heartbeat and ensurePartitions
        final ScheduledExecutorService serviceScheduledExecutor = Executors.newScheduledThreadPool(2,
                new NamedThreadFactory(threadGroup, "service"));

        final ScheduledExecutorService scheduledBootstrapExecutor = Executors.newScheduledThreadPool(config.getPartitionScheduledBootstrapThreads(),
                new NamedThreadFactory(threadGroup, "scheduled_bootstrap"));

        final ScheduledExecutorService scheduledRebuildExecutor = Executors.newScheduledThreadPool(config.getPartitionScheduledRebuildThreads(),
                new NamedThreadFactory(threadGroup, "scheduled_rebuild"));

        final ScheduledExecutorService scheduledSipMigrateExecutor = Executors.newScheduledThreadPool(config.getPartitionScheduledSipMigrateThreads(),
                new NamedThreadFactory(threadGroup, "scheduled_sip_migrate"));

        // query solvers
        final ExecutorService solverExecutor = Executors.newFixedThreadPool(config.getSolverExecutorThreads(),
                new NamedThreadFactory(threadGroup, "solver"));

        final ExecutorService rebuildExecutors = Executors.newFixedThreadPool(config.getRebuilderThreads(),
                new NamedThreadFactory(threadGroup, "hbase_rebuild_consumer"));

        final ExecutorService rebuildIndexExecutor = Executors.newFixedThreadPool(config.getRebuildIndexerThreads(),
                new NamedThreadFactory(threadGroup, "rebuild_index"));

        final ExecutorService sipIndexExecutor = Executors.newFixedThreadPool(config.getSipIndexerThreads(),
                new NamedThreadFactory(threadGroup, "sip_index"));

        final ExecutorService streamFactoryExecutor = Executors.newFixedThreadPool(config.getStreamFactoryExecutorCount(),
                new NamedThreadFactory(threadGroup, "stream_factory"));

        MiruHostedPartitionComparison partitionComparison = new MiruHostedPartitionComparison(
                config.getLongTailSolverWindowSize(),
                config.getLongTailSolverPercentile());

        MiruReadTrackingWALReader readTrackingWALReader = new MiruReadTrackingWALReaderImpl(wal.getReadTrackingWAL(), wal.getReadTrackingSipWAL());

        MiruContextFactory streamFactory = new MiruContextFactory(
                schemaProvider,
                streamFactoryExecutor,
                readTrackingWALReader,
                resourceLocatorProvider.getDiskResourceLocator(),
                resourceLocatorProvider.getTransientResourceLocator(),
                config.getPartitionAuthzCacheSize(),
                config.getHybridFieldInitialPageCapacity(),
                MiruBackingStorage.valueOf(config.getDefaultStorage()),
                internExtern);

        MiruActivityWALReader activityWALReader = new MiruActivityWALReaderImpl(wal.getActivityWAL(), wal.getActivitySipWAL());
        MiruPartitionEventHandler partitionEventHandler = new MiruPartitionEventHandler(clusterRegistry);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new GuavaModule());

        MiruPartitionInfoProvider partitionInfoProvider = new CachedClusterPartitionInfoProvider();

        ByteBufferFactory byteBufferFactory;
        if (config.getUseOffHeapBuffers()) {
            byteBufferFactory = new HeapByteBufferFactory();
        } else {
            byteBufferFactory = new DirectByteBufferFactory();
        }

        MiruLocalPartitionFactory localPartitionFactory = new MiruLocalPartitionFactory(new CurrentTimestamper(),
                config,
                streamFactory,
                activityWALReader,
                partitionEventHandler,
                byteBufferFactory,
                scheduledBootstrapExecutor,
                scheduledRebuildExecutor,
                scheduledSipMigrateExecutor,
                rebuildExecutors,
                rebuildIndexExecutor,
                sipIndexExecutor);

        MiruRemotePartitionFactory remotePartitionFactory = new MiruRemotePartitionFactory(partitionInfoProvider,
                httpClientFactory,
                objectMapper);

        MiruTenantTopologyFactory tenantTopologyFactory = new MiruTenantTopologyFactory(config,
                bitmapsProvider,
                miruHost,
                localPartitionFactory,
                remotePartitionFactory,
                partitionComparison);

        MiruExpectedTenants expectedTenants = new MiruClusterExpectedTenants(partitionInfoProvider,
                tenantTopologyFactory,
                clusterRegistry);

        final MiruClusterPartitionDirector partitionDirector = new MiruClusterPartitionDirector(miruHost, clusterRegistry, expectedTenants);
        MiruActivityWALWriter activityWALWriter = new MiruWriteToActivityAndSipWAL(wal.getActivityWAL(), wal.getActivitySipWAL());
        MiruActivityLookupTable activityLookupTable = new MiruRCVSActivityLookupTable(registryStore.getActivityLookupTable());

        MiruSolver solver = new MiruLowestLatencySolver(solverExecutor,
                config.getDefaultInitialSolvers(),
                config.getDefaultMaxNumberOfSolvers(),
                config.getDefaultAddAnotherSolverAfterNMillis(),
                config.getDefaultFailAfterNMillis());

        final MiruService miruService = new MiruService(
                miruHost,
                partitionDirector,
                partitionComparison,
                activityWALWriter,
                activityLookupTable,
                solver,
                schemaProvider);

        return new MiruLifecyle<MiruService>() {

            @Override
            public MiruService getService() {
                return miruService;
            }

            @Override
            public void start() throws Exception {
                long heartbeatInterval = config.getHeartbeatIntervalInMillis();
                serviceScheduledExecutor.scheduleWithFixedDelay(new Runnable() {

                    @Override
                    public void run() {
                        partitionDirector.heartbeat();
                    }
                }, 0, heartbeatInterval, TimeUnit.MILLISECONDS);
                long ensurePartitionsInterval = config.getEnsurePartitionsIntervalInMillis();
                serviceScheduledExecutor.scheduleWithFixedDelay(new Runnable() {

                    @Override
                    public void run() {
                        partitionDirector.ensureServerPartitions();
                    }
                }, 0, ensurePartitionsInterval, TimeUnit.MILLISECONDS);
            }

            @Override
            public void stop() throws Exception {
                serviceScheduledExecutor.shutdownNow();
                scheduledBootstrapExecutor.shutdownNow();
                scheduledRebuildExecutor.shutdownNow();
                scheduledSipMigrateExecutor.shutdownNow();
                solverExecutor.shutdownNow();
                streamFactoryExecutor.shutdownNow();
            }
        };
    }

    private static class NamedThreadFactory implements ThreadFactory {

        private final ThreadGroup g;
        private final String namePrefix;
        private final AtomicLong threadNumber;

        public NamedThreadFactory(ThreadGroup g, String name) {
            this.g = g;
            namePrefix = name + "-";
            threadNumber = new AtomicLong();
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(g, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}
