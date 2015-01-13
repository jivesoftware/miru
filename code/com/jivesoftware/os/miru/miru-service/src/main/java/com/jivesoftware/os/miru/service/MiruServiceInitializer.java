package com.jivesoftware.os.miru.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.DirectByteBufferFactory;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.logger.MetricLogger;
import com.jivesoftware.os.jive.utils.logger.MetricLoggerFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.cluster.MiruActivityLookupTable;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryStore;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSActivityLookupTable;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsProvider;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.service.locator.MiruHybridResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocatorProvider;
import com.jivesoftware.os.miru.service.partition.MiruClusterPartitionDirector;
import com.jivesoftware.os.miru.service.partition.MiruExpectedTenants;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartitionComparison;
import com.jivesoftware.os.miru.service.partition.MiruIndexRepairs;
import com.jivesoftware.os.miru.service.partition.MiruLocalPartitionFactory;
import com.jivesoftware.os.miru.service.partition.MiruPartitionAccessor.IndexStrategy;
import com.jivesoftware.os.miru.service.partition.MiruPartitionEventHandler;
import com.jivesoftware.os.miru.service.partition.MiruPartitionInfoProvider;
import com.jivesoftware.os.miru.service.partition.MiruRemotePartitionFactory;
import com.jivesoftware.os.miru.service.partition.MiruTenantTopologyFactory;
import com.jivesoftware.os.miru.service.partition.cluster.CachedClusterPartitionInfoProvider;
import com.jivesoftware.os.miru.service.partition.cluster.MiruClusterExpectedTenants;
import com.jivesoftware.os.miru.service.solver.MiruLowestLatencySolver;
import com.jivesoftware.os.miru.service.solver.MiruSolver;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.service.stream.MiruRebuildDirector;
import com.jivesoftware.os.miru.service.stream.allocator.HybridMiruContextAllocator;
import com.jivesoftware.os.miru.service.stream.allocator.MiruContextAllocator;
import com.jivesoftware.os.miru.service.stream.allocator.OnDiskMiruContextAllocator;
import com.jivesoftware.os.miru.wal.MiruWALInitializer.MiruWAL;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReaderImpl;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.MiruWriteToActivityAndSipWAL;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MiruServiceInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

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

        final ExecutorService sipIndexExecutor = Executors.newFixedThreadPool(config.getSipIndexerThreads(),
            new NamedThreadFactory(threadGroup, "sip_index"));

        final ExecutorService streamFactoryExecutor = Executors.newFixedThreadPool(config.getStreamFactoryExecutorCount(),
            new NamedThreadFactory(threadGroup, "stream_factory"));

        MiruHostedPartitionComparison partitionComparison = new MiruHostedPartitionComparison(
            config.getLongTailSolverWindowSize(),
            config.getLongTailSolverPercentile());

        MiruReadTrackingWALReader readTrackingWALReader = new MiruReadTrackingWALReaderImpl(wal.getReadTrackingWAL(), wal.getReadTrackingSipWAL());

        ByteBufferFactory byteBufferFactory;
        if (config.getUseOffHeapBuffers()) {
            byteBufferFactory = new DirectByteBufferFactory();
        } else {
            byteBufferFactory = new HeapByteBufferFactory();
        }

        StripingLocksProvider<MiruTermId> fieldIndexStripingLocksProvider = new StripingLocksProvider<>(config.getFieldIndexNumberOfLocks());
        StripingLocksProvider<MiruStreamId> streamStripingLocksProvider = new StripingLocksProvider<>(config.getStreamNumberOfLocks());
        StripingLocksProvider<String> authzStripingLocksProvider = new StripingLocksProvider<>(config.getAuthzNumberOfLocks());
        StripingLocksProvider<Long> chunkStripingLocksProvider = new StripingLocksProvider<>(config.getChunkStoreNumberOfLocks());

        MiruHybridResourceLocator transientResourceLocator = resourceLocatorProvider.getTransientResourceLocator();
        MiruContextAllocator hybridContextAllocator = new HybridMiruContextAllocator(schemaProvider,
            internExtern,
            readTrackingWALReader,
            transientResourceLocator,
            byteBufferFactory,
            config.getPartitionNumberOfChunkStores(),
            config.getPartitionAuthzCacheSize(),
            config.getPartitionDeleteChunkStoreOnClose(),
            fieldIndexStripingLocksProvider,
            streamStripingLocksProvider, authzStripingLocksProvider, chunkStripingLocksProvider);

        final MiruResourceLocator diskResourceLocator = resourceLocatorProvider.getDiskResourceLocator();
        MiruContextAllocator diskContextAllocator = new OnDiskMiruContextAllocator(schemaProvider,
            internExtern,
            readTrackingWALReader,
            diskResourceLocator,
            config.getPartitionNumberOfChunkStores(),
            config.getPartitionAuthzCacheSize(),
            fieldIndexStripingLocksProvider,
            streamStripingLocksProvider,
            authzStripingLocksProvider,
            chunkStripingLocksProvider);

        MiruContextFactory streamFactory = new MiruContextFactory(
            ImmutableMap.<MiruBackingStorage, MiruContextAllocator>builder()
                .put(MiruBackingStorage.memory, hybridContextAllocator)
                .put(MiruBackingStorage.memory_fixed, hybridContextAllocator)
                .put(MiruBackingStorage.hybrid, hybridContextAllocator)
                .put(MiruBackingStorage.hybrid_fixed, hybridContextAllocator)
                .put(MiruBackingStorage.mem_mapped, diskContextAllocator)
                .put(MiruBackingStorage.disk, diskContextAllocator)
                .build(),
            diskResourceLocator,
            transientResourceLocator,
            MiruBackingStorage.valueOf(config.getDefaultStorage()));

        MiruActivityWALReader activityWALReader = new MiruActivityWALReaderImpl(wal.getActivityWAL(), wal.getActivitySipWAL());
        MiruPartitionEventHandler partitionEventHandler = new MiruPartitionEventHandler(clusterRegistry);
        MiruRebuildDirector rebuildDirector = new MiruRebuildDirector(config.getMaxRebuildActivityCount());
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.registerModule(new GuavaModule());

        MiruPartitionInfoProvider partitionInfoProvider = new CachedClusterPartitionInfoProvider();

        MiruIndexRepairs indexRepairs = new MiruIndexRepairs() {
            private final AtomicBoolean current = new AtomicBoolean(false);
            private final Set<IndexStrategy> monitorStrategies = Sets.newHashSet(IndexStrategy.sip, IndexStrategy.rebuild);

            @Override
            public void repaired(IndexStrategy strategy, MiruPartitionCoord coord, int numberRepaired) {
                if (monitorStrategies.contains(strategy) && numberRepaired > 0 && current.compareAndSet(true, false)) {
                    LOG.warn("strategy:" + strategy + " coord:" + coord + " is NOT consistent.");
                }
            }

            @Override
            public void current(IndexStrategy strategy, MiruPartitionCoord coord) {
                if (monitorStrategies.contains(strategy) && current.compareAndSet(false, true)) {
                    LOG.info("strategy:" + strategy + " coord:" + coord + " is consistent.");
                }
            }
        };

        MiruLocalPartitionFactory localPartitionFactory = new MiruLocalPartitionFactory(config,
            streamFactory,
            activityWALReader,
            partitionEventHandler,
            rebuildDirector,
            scheduledBootstrapExecutor,
            scheduledRebuildExecutor,
            scheduledSipMigrateExecutor,
            rebuildExecutors,
            sipIndexExecutor,
            config.getRebuildIndexerThreads(),
            indexRepairs);
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

}
