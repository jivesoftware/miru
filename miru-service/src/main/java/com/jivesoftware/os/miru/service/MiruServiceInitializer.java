package com.jivesoftware.os.miru.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.chunk.store.transaction.KeyToFPCacheFactory;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCogs;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.DirectByteBufferFactory;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LabHeapPressure;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.lab.guts.StripingBolBufferLocks;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.realtime.MiruRealtimeDelivery;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.plugin.MiruInterner;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsProvider;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndexMarshaller;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.service.index.TimeIdIndex;
import com.jivesoftware.os.miru.service.index.lab.LabTimeIdIndexInitializer;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
import com.jivesoftware.os.miru.service.partition.FreeMergeChits;
import com.jivesoftware.os.miru.service.partition.LargestFirstMergeChits;
import com.jivesoftware.os.miru.service.partition.MiruClusterPartitionDirector;
import com.jivesoftware.os.miru.service.partition.MiruExpectedTenants;
import com.jivesoftware.os.miru.service.partition.MiruHostedPartitionComparison;
import com.jivesoftware.os.miru.service.partition.MiruIndexRepairs;
import com.jivesoftware.os.miru.service.partition.MiruLocalPartitionFactory;
import com.jivesoftware.os.miru.service.partition.MiruMergeChits;
import com.jivesoftware.os.miru.service.partition.MiruPartitionAccessor.IndexStrategy;
import com.jivesoftware.os.miru.service.partition.MiruPartitionHeartbeatHandler;
import com.jivesoftware.os.miru.service.partition.MiruRemoteQueryablePartitionFactory;
import com.jivesoftware.os.miru.service.partition.MiruSipTrackerFactory;
import com.jivesoftware.os.miru.service.partition.MiruTenantTopologyFactory;
import com.jivesoftware.os.miru.service.partition.PartitionErrorTracker;
import com.jivesoftware.os.miru.service.partition.cluster.MiruClusterExpectedTenants;
import com.jivesoftware.os.miru.service.solver.MiruLowestLatencySolver;
import com.jivesoftware.os.miru.service.solver.MiruSolver;
import com.jivesoftware.os.miru.service.stream.LabPluginCacheProvider;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.service.stream.MiruRebuildDirector;
import com.jivesoftware.os.miru.service.stream.allocator.InMemoryChunkAllocator;
import com.jivesoftware.os.miru.service.stream.allocator.MiruChunkAllocator;
import com.jivesoftware.os.miru.service.stream.allocator.OnDiskChunkAllocator;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MiruServiceInitializer {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public <C extends MiruCursor<C, S>, S extends MiruSipCursor<S>> MiruLifecyle<MiruService> initialize(final MiruServiceConfig config,
        MiruStats miruStats,
        ScheduledExecutorService scheduledBootstrapExecutor,
        ScheduledExecutorService scheduledRebuildExecutor,
        ScheduledExecutorService scheduledSipMigrateExecutor,
        MiruClusterClient clusterClient,
        MiruHost miruHost,
        MiruSchemaProvider schemaProvider,
        MiruWALClient<C, S> walClient,
        MiruRealtimeDelivery realtimeDelivery,
        MiruSipTrackerFactory<S> sipTrackerFactory,
        MiruSipIndexMarshaller<S> sipIndexMarshaller,
        MiruResourceLocator resourceLocator,
        MiruTermComposer termComposer,
        MiruActivityInternExtern internExtern,
        MiruBitmapsProvider bitmapsProvider,
        PartitionErrorTracker partitionErrorTracker,
        MiruInterner<MiruTermId> termInterner) throws Exception {

        final ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();

        // heartbeat and ensurePartitions
        final ScheduledExecutorService serviceScheduledExecutor = Executors.newScheduledThreadPool(2,
            new NamedThreadFactory(threadGroup, "service"));

        // query solvers
        final ExecutorService solverExecutor = Executors.newFixedThreadPool(config.getSolverExecutorThreads(),
            new NamedThreadFactory(threadGroup, "solver"));

        final ExecutorService parallelExecutor = Executors.newFixedThreadPool(config.getParallelSolversExecutorThreads(),
            new NamedThreadFactory(threadGroup, "parallel_solvers"));

        final ExecutorService rebuildExecutors = Executors.newFixedThreadPool(config.getRebuilderThreads(),
            new NamedThreadFactory(threadGroup, "rebuild_wal_consumer"));

        final ExecutorService sipIndexExecutor = Executors.newFixedThreadPool(config.getSipIndexerThreads(),
            new NamedThreadFactory(threadGroup, "sip_index"));

        final ExecutorService persistentMergeExecutor = Executors.newFixedThreadPool(config.getMergeIndexThreads(),
            new NamedThreadFactory(threadGroup, "persistent_merge_index"));

        final ExecutorService transientMergeExecutor = Executors.newFixedThreadPool(config.getMergeIndexThreads(),
            new NamedThreadFactory(threadGroup, "transient_merge_index"));

        final ExecutorService streamFactoryExecutor = Executors.newFixedThreadPool(config.getStreamFactoryExecutorCount(),
            new NamedThreadFactory(threadGroup, "stream_factory"));

        MiruHostedPartitionComparison partitionComparison = new MiruHostedPartitionComparison(
            config.getLongTailSolverWindowSize(),
            config.getLongTailSolverPercentile());

        ByteBufferFactory byteBufferFactory;
        if (config.getUseOffHeapBuffers()) {
            byteBufferFactory = new DirectByteBufferFactory();
        } else {
            byteBufferFactory = new HeapByteBufferFactory();
        }

        StripingLocksProvider<MiruTermId> fieldIndexStripingLocksProvider = new StripingLocksProvider<>(config.getFieldIndexNumberOfLocks());
        StripingLocksProvider<MiruStreamId> streamStripingLocksProvider = new StripingLocksProvider<>(config.getStreamNumberOfLocks());
        StripingLocksProvider<String> authzStripingLocksProvider = new StripingLocksProvider<>(config.getAuthzNumberOfLocks());

        ExecutorService inMemoryLabHeapScheduler = LABEnvironment.buildLABHeapSchedulerThreadPool(config.getRebuildLabHeapPressureStripes());
        AtomicLong inMemoryLabHeapCostInBytes = new AtomicLong();
        LabHeapPressure[] inMemoryLabHeapPressures = new LabHeapPressure[config.getRebuildLabHeapPressureStripes()];
        for (int i = 0; i < inMemoryLabHeapPressures.length; i++) {
            inMemoryLabHeapPressures[i] = new LabHeapPressure(inMemoryLabHeapScheduler,
                "rebuild-" + i,
                config.getRebuildLabMaxHeapPressureInBytes(),
                config.getRebuildLabBlockOnHeapPressureInBytes(),
                inMemoryLabHeapCostInBytes);
        }

        ExecutorService onDiskLabHeapScheduler = LABEnvironment.buildLABHeapSchedulerThreadPool(config.getGlobalLabHeapPressureStripes());
        AtomicLong onDiskLabHeapCostInBytes = new AtomicLong();
        LabHeapPressure[] onDiskLabHeapPressures = new LabHeapPressure[config.getGlobalLabHeapPressureStripes()];
        for (int i = 0; i < onDiskLabHeapPressures.length; i++) {
            onDiskLabHeapPressures[i] = new LabHeapPressure(onDiskLabHeapScheduler,
                "global-" + i,
                config.getGlobalLabMaxHeapPressureInBytes(),
                config.getGlobalLabBlockOnHeapPressureInBytes(),
                onDiskLabHeapCostInBytes);
        }

        LRUConcurrentBAHLinkedHash<Leaps> leapCache = LABEnvironment.buildLeapsCache((int) config.getLabLeapCacheMaxCapacity(),
            config.getLabLeapCacheConcurrency());

        StripingBolBufferLocks bolBufferLocks = new StripingBolBufferLocks(2048); // TODO config

        MiruChunkAllocator inMemoryChunkAllocator = new InMemoryChunkAllocator(resourceLocator,
            byteBufferFactory,
            byteBufferFactory,
            resourceLocator.getInMemoryChunkSize(),
            config.getPartitionNumberOfChunkStores(),
            config.getPartitionDeleteChunkStoreOnClose(),
            config.getPartitionInitialChunkCacheSize(),
            config.getPartitionMaxChunkCacheSize(),
            inMemoryLabHeapPressures,
            config.getLabMaxWALSizeInBytes(),
            config.getLabMaxEntriesPerWAL(),
            config.getLabMaxEntrySizeInBytes(),
            config.getLabMaxWALOnOpenHeapPressureOverride(),
            config.getLabUseOffHeap(),
            config.getUseLabIndexes(),
            leapCache,
            bolBufferLocks);

        MiruChunkAllocator onDiskChunkAllocator = new OnDiskChunkAllocator(resourceLocator,
            byteBufferFactory,
            config.getPartitionNumberOfChunkStores(),
            config.getPartitionInitialChunkCacheSize(),
            config.getPartitionMaxChunkCacheSize(),
            onDiskLabHeapPressures,
            config.getLabMaxWALSizeInBytes(),
            config.getLabMaxEntriesPerWAL(),
            config.getLabMaxEntrySizeInBytes(),
            config.getLabMaxWALOnOpenHeapPressureOverride(),
            config.getLabUseOffHeap(),
            leapCache,
            bolBufferLocks);

        TxCogs persistentCogs = new TxCogs(1024, 1024,
            new ConcurrentKeyToFPCacheFactory(),
            new NullKeyToFPCacheFactory(),
            new NullKeyToFPCacheFactory());

        TxCogs transientCogs = new TxCogs(1024, 1024,
            new ConcurrentKeyToFPCacheFactory(),
            new NullKeyToFPCacheFactory(),
            new NullKeyToFPCacheFactory());

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.registerModule(new GuavaModule());

        OrderIdProvider idProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));

        TimeIdIndex[] timeIdIndexes = new LabTimeIdIndexInitializer().initialize(config.getTimeIdKeepNIndexes(),
            config.getTimeIdMaxEntriesPerIndex(),
            config.getTimeIdMaxHeapPressureInBytes(),
            config.getTimeIdFsyncOnAppend(),
            resourceLocator,
            onDiskChunkAllocator);

        MiruContextFactory<S> contextFactory = new MiruContextFactory<>(idProvider,
            persistentCogs,
            transientCogs,
            timeIdIndexes,
            LabPluginCacheProvider.allocateLocks(Short.MAX_VALUE),
            schemaProvider,
            termComposer,
            internExtern,
            ImmutableMap.<MiruBackingStorage, MiruChunkAllocator>builder()
                .put(MiruBackingStorage.memory, inMemoryChunkAllocator)
                .put(MiruBackingStorage.disk, onDiskChunkAllocator)
                .build(),
            sipIndexMarshaller,
            resourceLocator,
            config.getPartitionAuthzCacheSize(),
            fieldIndexStripingLocksProvider,
            streamStripingLocksProvider,
            authzStripingLocksProvider,
            partitionErrorTracker,
            termInterner,
            objectMapper,
            config.getLabMaxHeapPressureInBytes(),
            config.getUseLabIndexes(),
            config.getRealtimeDelivery(),
            config.getFsyncOnCommit());

        MiruPartitionHeartbeatHandler heartbeatHandler = new MiruPartitionHeartbeatHandler(clusterClient);
        MiruRebuildDirector rebuildDirector = new MiruRebuildDirector(config.getMaxRebuildActivityCount());

        MiruIndexRepairs indexRepairs = new MiruIndexRepairs() {
            private final AtomicBoolean current = new AtomicBoolean(false);
            private final Set<IndexStrategy> monitorStrategies = Sets.newHashSet(IndexStrategy.sip, IndexStrategy.rebuild);

            @Override
            public void repaired(IndexStrategy strategy, MiruPartitionCoord coord, int numberRepaired) {
                if (monitorStrategies.contains(strategy) && numberRepaired > 0 && current.compareAndSet(true, false)) {
                    LOG.debug("strategy:{} coord:{} is NOT consistent.", strategy, coord);
                }
            }

            @Override
            public void current(IndexStrategy strategy, MiruPartitionCoord coord) {
                if (monitorStrategies.contains(strategy) && current.compareAndSet(false, true)) {
                    LOG.debug("strategy:{} coord:{} is consistent.", strategy, coord);
                }
            }
        };

        MiruMergeChits persistentMergeChits = new LargestFirstMergeChits("persistent", new AtomicLong(config.getPersistentMergeChitCount()));
        MiruMergeChits transientMergeChits = new FreeMergeChits("transient");
        MiruLocalPartitionFactory<C, S> localPartitionFactory = new MiruLocalPartitionFactory<>(miruStats,
            config,
            contextFactory,
            sipTrackerFactory,
            walClient,
            realtimeDelivery,
            heartbeatHandler,
            rebuildDirector,
            scheduledBootstrapExecutor,
            scheduledRebuildExecutor,
            scheduledSipMigrateExecutor,
            rebuildExecutors,
            sipIndexExecutor,
            persistentMergeExecutor,
            transientMergeExecutor,
            config.getRebuildIndexerThreads(),
            indexRepairs,
            persistentMergeChits,
            transientMergeChits,
            partitionErrorTracker);

        MiruRemoteQueryablePartitionFactory remotePartitionFactory = new MiruRemoteQueryablePartitionFactory();

        MiruTenantTopologyFactory tenantTopologyFactory = new MiruTenantTopologyFactory(config,
            bitmapsProvider,
            miruHost,
            localPartitionFactory);

        MiruExpectedTenants expectedTenants = new MiruClusterExpectedTenants(
            miruHost,
            tenantTopologyFactory,
            remotePartitionFactory,
            heartbeatHandler,
            partitionComparison,
            clusterClient);

        final MiruClusterPartitionDirector partitionDirector = new MiruClusterPartitionDirector(miruHost, expectedTenants);

        MiruSolver solver = new MiruLowestLatencySolver(config.getDefaultInitialSolvers(),
            config.getDefaultMaxNumberOfSolvers(),
            config.getDefaultAddAnotherSolverAfterNMillis(),
            config.getDefaultFailAfterNMillis());

        final MiruService miruService = new MiruService(
            miruHost,
            partitionDirector,
            partitionComparison,
            solver,
            schemaProvider,
            solverExecutor,
            parallelExecutor);

        return new MiruLifecyle<MiruService>() {

            @Override
            public MiruService getService() {
                return miruService;
            }

            @Override
            public void start() throws Exception {
                long heartbeatInterval = config.getHeartbeatIntervalInMillis();
                serviceScheduledExecutor.scheduleWithFixedDelay(partitionDirector::heartbeat, 0, heartbeatInterval, TimeUnit.MILLISECONDS);
                leapCache.start("contextCache", config.getLabLeapCacheCleanupIntervalMillis(), throwable -> {
                    LOG.error("Failure in LAB leap cache cleaner", throwable);
                    return false;
                });
            }

            @Override
            public void stop() throws Exception {
                serviceScheduledExecutor.shutdownNow();
                scheduledBootstrapExecutor.shutdownNow();
                scheduledRebuildExecutor.shutdownNow();
                scheduledSipMigrateExecutor.shutdownNow();
                solverExecutor.shutdownNow();
                parallelExecutor.shutdownNow();
                streamFactoryExecutor.shutdownNow();
                leapCache.stop();
            }
        };
    }

    private static class ConcurrentKeyToFPCacheFactory implements KeyToFPCacheFactory {

        @Override
        public Map<IBA, Long> createCache() {
            return new ConcurrentHashMap<>();
        }
    }

    private static class NullKeyToFPCacheFactory implements KeyToFPCacheFactory {

        @Override
        public Map<IBA, Long> createCache() {
            return null;
        }
    }
}
