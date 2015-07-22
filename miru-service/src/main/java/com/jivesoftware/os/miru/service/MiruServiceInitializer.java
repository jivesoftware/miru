package com.jivesoftware.os.miru.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.chunk.store.transaction.KeyToFPCacheFactory;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCogs;
import com.jivesoftware.os.filer.io.ByteBufferFactory;
import com.jivesoftware.os.filer.io.DirectByteBufferFactory;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.IBA;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruLifecyle;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.wal.MiruCursor;
import com.jivesoftware.os.miru.api.wal.MiruSipCursor;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmapsProvider;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndexMarshaller;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.service.locator.MiruResourceLocator;
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
import com.jivesoftware.os.miru.service.partition.cluster.MiruClusterExpectedTenants;
import com.jivesoftware.os.miru.service.solver.MiruLowestLatencySolver;
import com.jivesoftware.os.miru.service.solver.MiruSolver;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.service.stream.MiruRebuildDirector;
import com.jivesoftware.os.miru.service.stream.allocator.InMemoryChunkAllocator;
import com.jivesoftware.os.miru.service.stream.allocator.MiruChunkAllocator;
import com.jivesoftware.os.miru.service.stream.allocator.OnDiskChunkAllocator;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpClientFactory;
import java.io.IOException;
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
        MiruClusterClient clusterClient,
        MiruHost miruHost,
        MiruSchemaProvider schemaProvider,
        MiruWALClient<C, S> walClient,
        MiruSipTrackerFactory<S> sipTrackerFactory,
        MiruSipIndexMarshaller<S> sipIndexMarshaller,
        HttpClientFactory httpClientFactory,
        MiruResourceLocator resourceLocator,
        MiruTermComposer termComposer,
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
            new NamedThreadFactory(threadGroup, "rebuild_wal_consumer"));

        final ExecutorService sipIndexExecutor = Executors.newFixedThreadPool(config.getSipIndexerThreads(),
            new NamedThreadFactory(threadGroup, "sip_index"));

        final ExecutorService mergeExecutor = Executors.newFixedThreadPool(config.getMergeIndexThreads(),
            new NamedThreadFactory(threadGroup, "merge_index"));

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

        MiruChunkAllocator inMemoryChunkAllocator = new InMemoryChunkAllocator(
            byteBufferFactory,
            byteBufferFactory,
            resourceLocator.getInMemoryChunkSize(),
            config.getPartitionNumberOfChunkStores(),
            config.getPartitionNumberOfIndexStores(),
            config.getPartitionDeleteChunkStoreOnClose(),
            config.getPartitionDeleteIndexStoreOnClose(),
            config.getPartitionInitialChunkCacheSize(),
            config.getPartitionMaxChunkCacheSize());

        MiruChunkAllocator onDiskChunkAllocator = new OnDiskChunkAllocator(resourceLocator,
            byteBufferFactory,
            config.getPartitionNumberOfChunkStores(),
            config.getPartitionNumberOfIndexStores(),
            config.getPartitionInitialChunkCacheSize(),
            config.getPartitionMaxChunkCacheSize());

        Cache<MiruFieldIndex.IndexKey, Optional<?>> fieldIndexCache = CacheBuilder.newBuilder()
            .expireAfterAccess(config.getFieldIndexCacheExpireAfterMillis(), TimeUnit.MILLISECONDS)
            .maximumSize(config.getFieldIndexCacheMaxSize())
            .concurrencyLevel(config.getFieldIndexCacheConcurrencyLevel())
            .softValues()
            .build();

        TxCogs cogs = new TxCogs(256, 64,
            new ConcurrentKeyToFPCacheFactory(),
            new NullKeyToFPCacheFactory(),
            new NullKeyToFPCacheFactory());

        MiruContextFactory<S> contextFactory = new MiruContextFactory<>(cogs,
            schemaProvider,
            termComposer,
            internExtern,
            ImmutableMap.<MiruBackingStorage, MiruChunkAllocator>builder()
                .put(MiruBackingStorage.memory, inMemoryChunkAllocator)
                .put(MiruBackingStorage.disk, onDiskChunkAllocator)
                .build(),
            sipIndexMarshaller,
            resourceLocator,
            MiruBackingStorage.valueOf(config.getDefaultStorage()),
            config.getPartitionAuthzCacheSize(),
            fieldIndexCache,
            new AtomicLong(0),
            fieldIndexStripingLocksProvider,
            streamStripingLocksProvider,
            authzStripingLocksProvider
        );

        MiruPartitionHeartbeatHandler heartbeatHandler = new MiruPartitionHeartbeatHandler(clusterClient);
        MiruRebuildDirector rebuildDirector = new MiruRebuildDirector(config.getMaxRebuildActivityCount());
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.registerModule(new GuavaModule());

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

        MiruMergeChits miruMergeChits = new MiruMergeChits(config.getMergeChitCount(), config.getMergeMaxOverage());
        MiruLocalPartitionFactory<C, S> localPartitionFactory = new MiruLocalPartitionFactory<>(miruStats,
            config,
            contextFactory,
            sipTrackerFactory,
            walClient,
            heartbeatHandler,
            rebuildDirector,
            scheduledBootstrapExecutor,
            scheduledRebuildExecutor,
            scheduledSipMigrateExecutor,
            rebuildExecutors,
            sipIndexExecutor,
            mergeExecutor,
            config.getRebuildIndexerThreads(),
            indexRepairs,
            miruMergeChits);

        MiruRemoteQueryablePartitionFactory remotePartitionFactory = new MiruRemoteQueryablePartitionFactory(httpClientFactory);

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

        final MiruClusterPartitionDirector partitionDirector = new MiruClusterPartitionDirector(miruHost, clusterClient, expectedTenants);

        MiruSolver solver = new MiruLowestLatencySolver(solverExecutor,
            config.getDefaultInitialSolvers(),
            config.getDefaultMaxNumberOfSolvers(),
            config.getDefaultAddAnotherSolverAfterNMillis(),
            config.getDefaultFailAfterNMillis());

        final MiruService miruService = new MiruService(
            miruHost,
            partitionDirector,
            partitionComparison,
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
                serviceScheduledExecutor.scheduleWithFixedDelay(partitionDirector::heartbeat, 0, heartbeatInterval, TimeUnit.MILLISECONDS);
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
