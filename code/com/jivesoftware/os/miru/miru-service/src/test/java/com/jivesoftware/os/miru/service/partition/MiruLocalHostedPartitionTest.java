package com.jivesoftware.os.miru.service.partition;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Interners;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.filer.chunk.store.transaction.TxCogs;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.io.StripingLocksProvider;
import com.jivesoftware.os.jive.utils.health.api.HealthCheckConfigBinder;
import com.jivesoftware.os.jive.utils.health.api.HealthCheckRegistry;
import com.jivesoftware.os.jive.utils.health.api.HealthChecker;
import com.jivesoftware.os.jive.utils.health.api.HealthFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordInfo;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.MiruStats;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchemaUnvailableException;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruStreamId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.topology.MiruClusterClient;
import com.jivesoftware.os.miru.api.wal.MiruWALClient;
import com.jivesoftware.os.miru.cluster.MiruClusterRegistry;
import com.jivesoftware.os.miru.cluster.MiruRegistryClusterClient;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.plugin.index.BloomIndex;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.locator.MiruTempDirectoryResourceLocator;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.service.stream.MiruIndexAuthz;
import com.jivesoftware.os.miru.service.stream.MiruIndexBloom;
import com.jivesoftware.os.miru.service.stream.MiruIndexFieldValues;
import com.jivesoftware.os.miru.service.stream.MiruIndexLatest;
import com.jivesoftware.os.miru.service.stream.MiruIndexPairedLatest;
import com.jivesoftware.os.miru.service.stream.MiruIndexer;
import com.jivesoftware.os.miru.service.stream.MiruRebuildDirector;
import com.jivesoftware.os.miru.service.stream.allocator.InMemoryChunkAllocator;
import com.jivesoftware.os.miru.service.stream.allocator.MiruChunkAllocator;
import com.jivesoftware.os.miru.service.stream.allocator.OnDiskChunkAllocator;
import com.jivesoftware.os.miru.wal.MiruWALDirector;
import com.jivesoftware.os.miru.wal.MiruWALInitializer;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALWriter;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruRCVSActivityWALReader;
import com.jivesoftware.os.miru.wal.activity.rcvs.MiruRCVSActivityWALWriter;
import com.jivesoftware.os.miru.wal.lookup.MiruActivityLookupTable;
import com.jivesoftware.os.miru.wal.lookup.MiruRCVSActivityLookupTable;
import com.jivesoftware.os.miru.wal.partition.MiruPartitionIdProvider;
import com.jivesoftware.os.miru.wal.partition.MiruRCVSPartitionIdProvider;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReader;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import com.jivesoftware.os.rcvs.api.timestamper.CurrentTimestamper;
import com.jivesoftware.os.rcvs.inmemory.InMemoryRowColumnValueStore;
import com.jivesoftware.os.rcvs.inmemory.InMemoryRowColumnValueStoreInitializer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.merlin.config.BindInterfaceToConfiguration;
import org.merlin.config.Config;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class MiruLocalHostedPartitionTest {

    private MiruContextFactory contextFactory;
    private MiruSchema schema;
    private MiruSchemaProvider schemaProvider;
    private MiruRCVSClusterRegistry clusterRegistry;
    private MiruPartitionCoord coord;
    private MiruBackingStorage defaultStorage;
    private MiruWALClient walClient;
    private MiruPartitionHeartbeatHandler partitionEventHandler;
    private MiruRebuildDirector rebuildDirector;
    private ScheduledExecutorService scheduledBootstrapService;
    private ScheduledExecutorService scheduledRebuildService;
    private ScheduledExecutorService scheduledSipMigrateService;
    private ExecutorService rebuildExecutor;
    private ExecutorService sipIndexExecutor;
    private ExecutorService mergeExecutor;
    private AtomicReference<MiruLocalHostedPartition.BootstrapRunnable> bootstrapRunnable;
    private AtomicReference<MiruLocalHostedPartition.RebuildIndexRunnable> rebuildIndexRunnable;
    private AtomicReference<MiruLocalHostedPartition.SipMigrateIndexRunnable> sipMigrateIndexRunnable;
    private MiruPartitionedActivityFactory factory;
    private MiruPartitionId partitionId;
    private MiruTenantId tenantId;
    private MiruHost host;
    private MiruBitmapsEWAH bitmaps;
    private MiruIndexer<EWAHCompressedBitmap> indexer;
    private MiruLocalHostedPartition.Timings timings;
    private long topologyIsStaleAfterMillis = TimeUnit.HOURS.toMillis(1);

    @BeforeMethod
    public void setUp() throws Exception {
        tenantId = new MiruTenantId("test".getBytes(Charsets.UTF_8));
        partitionId = MiruPartitionId.of(0);
        host = new MiruHost("localhost", 49_600);
        coord = new MiruPartitionCoord(tenantId, partitionId, host);
        defaultStorage = MiruBackingStorage.memory;

        HealthFactory.initialize(
            new HealthCheckConfigBinder() {
                @Override
                public <C extends Config> C bindConfig(Class<C> configurationInterfaceClass) {
                    return BindInterfaceToConfiguration.bindDefault(configurationInterfaceClass);
                }
            },
            new HealthCheckRegistry() {
                @Override
                public void register(HealthChecker healthChecker) {
                }

                @Override
                public void unregister(HealthChecker healthChecker) {
                }
            });

        MiruServiceConfig config = mock(MiruServiceConfig.class);
        when(config.getBitsetBufferSize()).thenReturn(32);
        when(config.getDefaultStorage()).thenReturn(defaultStorage.name());
        when(config.getPartitionNumberOfChunkStores()).thenReturn(1);
        when(config.getPartitionDeleteChunkStoreOnClose()).thenReturn(false);

        schema = new MiruSchema.Builder("test", 1)
            .setFieldDefinitions(DefaultMiruSchemaDefinition.FIELDS)
            .build();
        schemaProvider = mock(MiruSchemaProvider.class);
        when(schemaProvider.getSchema(any(MiruTenantId.class))).thenReturn(schema);

        bitmaps = new MiruBitmapsEWAH(2);
        indexer = new MiruIndexer<>(new MiruIndexAuthz<>(),
            new MiruIndexFieldValues<>(),
            new MiruIndexBloom<>(new BloomIndex<>(bitmaps, Hashing.murmur3_128(), 100_000, 0.01f)),
            new MiruIndexLatest<>(),
            new MiruIndexPairedLatest<>());
        timings = new MiruLocalHostedPartition.Timings(5_000, 5_000, 5_000, 30_000, 3_000);

        MiruTermComposer termComposer = new MiruTermComposer(Charsets.UTF_8);
        MiruActivityInternExtern activityInternExtern = new MiruActivityInternExtern(
            Interners.<MiruIBA>newWeakInterner(),
            Interners.<MiruTermId>newWeakInterner(),
            Interners.<MiruTenantId>newStrongInterner(),
            // makes sense to share string internment as this is authz in both cases
            Interners.<String>newWeakInterner(),
            termComposer);

        MiruChunkAllocator hybridContextAllocator = new InMemoryChunkAllocator(
            new HeapByteBufferFactory(),
            new HeapByteBufferFactory(),
            4_096,
            config.getPartitionNumberOfChunkStores(),
            config.getPartitionDeleteChunkStoreOnClose(),
            100,
            1_000);

        MiruChunkAllocator diskContextAllocator = new OnDiskChunkAllocator(
            new MiruTempDirectoryResourceLocator(),
            new HeapByteBufferFactory(),
            config.getPartitionNumberOfChunkStores(),
            100,
            1_000);

        TxCogs cogs = new TxCogs(256, 64, null, null, null);

        contextFactory = new MiruContextFactory(cogs,
            schemaProvider,
            termComposer,
            activityInternExtern,
            ImmutableMap.<MiruBackingStorage, MiruChunkAllocator>builder()
                .put(MiruBackingStorage.memory, hybridContextAllocator)
                .put(MiruBackingStorage.disk, diskContextAllocator)
                .build(),
            new MiruTempDirectoryResourceLocator(),
            defaultStorage,
            config.getPartitionAuthzCacheSize(),
            null,
            new AtomicLong(0),
            new StripingLocksProvider<MiruTermId>(8),
            new StripingLocksProvider<MiruStreamId>(8),
            new StripingLocksProvider<String>(8));

        clusterRegistry = new MiruRCVSClusterRegistry(
            new CurrentTimestamper(),
            new InMemoryRowColumnValueStore(),
            new InMemoryRowColumnValueStore(),
            new InMemoryRowColumnValueStore(),
            new InMemoryRowColumnValueStore(),
            new InMemoryRowColumnValueStore(),
            new InMemoryRowColumnValueStore(),
            new InMemoryRowColumnValueStore(),
            new InMemoryRowColumnValueStore(),
            3,
            topologyIsStaleAfterMillis,
            TimeUnit.HOURS.toMillis(1));

        MiruClusterClient clusterClient = new MiruRegistryClusterClient(clusterRegistry);
        clusterClient.elect(host, tenantId, partitionId, System.currentTimeMillis());

        partitionEventHandler = new MiruPartitionHeartbeatHandler(clusterClient);
        rebuildDirector = new MiruRebuildDirector(Long.MAX_VALUE);
        factory = new MiruPartitionedActivityFactory();

        scheduledBootstrapService = mock(ScheduledExecutorService.class);
        scheduledRebuildService = mock(ScheduledExecutorService.class);
        scheduledSipMigrateService = mock(ScheduledExecutorService.class);
        rebuildExecutor = Executors.newSingleThreadExecutor();
        sipIndexExecutor = Executors.newSingleThreadExecutor();
        mergeExecutor = Executors.newSingleThreadExecutor();

        bootstrapRunnable = new AtomicReference<>();
        captureRunnable(scheduledBootstrapService, bootstrapRunnable, MiruLocalHostedPartition.BootstrapRunnable.class);

        rebuildIndexRunnable = new AtomicReference<>();
        captureRunnable(scheduledRebuildService, rebuildIndexRunnable, MiruLocalHostedPartition.RebuildIndexRunnable.class);

        sipMigrateIndexRunnable = new AtomicReference<>();
        captureRunnable(scheduledSipMigrateService, sipMigrateIndexRunnable, MiruLocalHostedPartition.SipMigrateIndexRunnable.class);

        ObjectMapper mapper = new ObjectMapper();

        InMemoryRowColumnValueStoreInitializer inMemoryRowColumnValueStoreInitializer = new InMemoryRowColumnValueStoreInitializer();

        MiruWALInitializer.MiruWAL wal = new MiruWALInitializer().initialize("test", inMemoryRowColumnValueStoreInitializer, mapper);

        MiruActivityWALWriter activityWALWriter = new MiruRCVSActivityWALWriter(wal.getActivityWAL(), wal.getActivitySipWAL());
        MiruActivityWALReader activityWALReader = new MiruRCVSActivityWALReader(wal.getActivityWAL(), wal.getActivitySipWAL());
        MiruReadTrackingWALReader readTrackingWALReader = new MiruReadTrackingWALReaderImpl(wal.getReadTrackingWAL(), wal.getReadTrackingSipWAL());
        MiruActivityLookupTable activityLookupTable = new MiruRCVSActivityLookupTable(wal.getActivityLookupTable());
        MiruPartitionIdProvider miruPartitionIdProvider = new MiruRCVSPartitionIdProvider(1_000_000, wal.getWriterPartitionRegistry(), activityWALReader);

        walClient = new MiruWALDirector(activityLookupTable, activityWALReader, activityWALWriter, miruPartitionIdProvider,
            readTrackingWALReader);

    }

    @Test
    public void testBootstrapToOnline() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = getEwahCompressedBitmapMiruLocalHostedPartition(true);

        setActive(true);
        waitForRef(bootstrapRunnable).run();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.bootstrap);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);

        waitForRef(rebuildIndexRunnable).run();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.online);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);
        waitForRef(sipMigrateIndexRunnable).run();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.online);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.disk);

    }

    @Test
    public void testInactiveToOffline() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = getEwahCompressedBitmapMiruLocalHostedPartition(true);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // enters bootstrap
        waitForRef(rebuildIndexRunnable).run(); // enters rebuilding
        waitForRef(sipMigrateIndexRunnable).run(); // enters online mem-mapped

        setActive(false);
        waitForRef(bootstrapRunnable).run();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.disk);
    }

    @Test
    public void testQueryHandleOfflineMemMappedHotDeploy() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = getEwahCompressedBitmapMiruLocalHostedPartition(true);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // enters bootstrap
        waitForRef(rebuildIndexRunnable).run(); // enters rebuilding
        waitForRef(sipMigrateIndexRunnable).run(); // enters online memory
        indexBoundaryActivity(localHostedPartition); // eligible for disk
        waitForRef(sipMigrateIndexRunnable).run(); // enters online disk (hot deployable)

        setActive(false);
        waitForRef(bootstrapRunnable).run();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.disk);

        try (MiruRequestHandle queryHandle = localHostedPartition.acquireQueryHandle()) {
            assertEquals(queryHandle.getCoord(), coord);
            assertNotNull(queryHandle.getRequestContext()); // would throw exception if offline
        }
    }

    @Test(expectedExceptions = MiruPartitionUnavailableException.class)
    public void testQueryHandleOfflineMemoryException() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = getEwahCompressedBitmapMiruLocalHostedPartition(true);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // enters bootstrap
        waitForRef(rebuildIndexRunnable).run(); // enters rebuilding

        setActive(false);
        waitForRef(bootstrapRunnable).run();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);

        try (MiruRequestHandle queryHandle = localHostedPartition.acquireQueryHandle()) {
            queryHandle.getRequestContext(); // throws exception
        }
    }

    @Test
    public void testRemove() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = getEwahCompressedBitmapMiruLocalHostedPartition(true);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // enters bootstrap
        waitForRef(rebuildIndexRunnable).run(); // enters rebuilding
        waitForRef(sipMigrateIndexRunnable).run(); // enters online memory

        localHostedPartition.remove();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.disk);
    }

    @Test
    public void testWakeOnIndex_false() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = getEwahCompressedBitmapMiruLocalHostedPartition(false);

        indexNormalActivity(localHostedPartition);
        partitionEventHandler.thumpthump(host);
        waitForRef(bootstrapRunnable).run(); // stays offline

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);
    }

    @Test
    public void testWakeOnIndex_true() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = getEwahCompressedBitmapMiruLocalHostedPartition(true);

        indexNormalActivity(localHostedPartition);
        partitionEventHandler.thumpthump(host);
        waitForRef(bootstrapRunnable).run(); // enters bootstrap

        assertEquals(localHostedPartition.getState(), MiruPartitionState.bootstrap);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);
    }

    @Test
    public void testSchemaNotRegistered_checkActive() throws Exception {
        when(schemaProvider.getSchema(any(MiruTenantId.class))).thenThrow(new MiruSchemaUnvailableException("test"));
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = getEwahCompressedBitmapMiruLocalHostedPartition(false);

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), defaultStorage);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // enters bootstrap
        waitForRef(rebuildIndexRunnable).run(); // stays bootstrap
        waitForRef(sipMigrateIndexRunnable).run(); // stays bootstrap

        assertEquals(localHostedPartition.getState(), MiruPartitionState.bootstrap);
        assertEquals(localHostedPartition.getStorage(), defaultStorage);
    }

    @Test
    public void testSchemaRegisteredLate() throws Exception {
        when(schemaProvider.getSchema(any(MiruTenantId.class))).thenThrow(new MiruSchemaUnvailableException("test"));
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = getEwahCompressedBitmapMiruLocalHostedPartition(false);

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), defaultStorage);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // enters bootstrap
        waitForRef(rebuildIndexRunnable).run(); // stays bootstrap
        waitForRef(sipMigrateIndexRunnable).run(); // stays bootstrap

        assertEquals(localHostedPartition.getState(), MiruPartitionState.bootstrap);
        assertEquals(localHostedPartition.getStorage(), defaultStorage);

        reset(schemaProvider);
        when(schemaProvider.getSchema(any(MiruTenantId.class))).thenReturn(schema);

        waitForRef(bootstrapRunnable).run(); // stays bootstrap
        waitForRef(rebuildIndexRunnable).run(); // enters rebuilding, online memory
        waitForRef(sipMigrateIndexRunnable).run(); // enters online disk

        assertEquals(localHostedPartition.getState(), MiruPartitionState.online);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.disk);
    }

    private MiruLocalHostedPartition<EWAHCompressedBitmap> getEwahCompressedBitmapMiruLocalHostedPartition(boolean wakeOnIndex) throws Exception {
        return new MiruLocalHostedPartition<>(new MiruStats(), bitmaps, coord, -1, contextFactory,
            walClient, partitionEventHandler, rebuildDirector, scheduledBootstrapService, scheduledRebuildService,
            scheduledSipMigrateService, rebuildExecutor, sipIndexExecutor, mergeExecutor, 1, new NoOpMiruIndexRepairs(),
            indexer, wakeOnIndex, 100_000, 100, 100, new MiruMergeChits(100_000, 10_000), timings);
    }

    private void setActive(boolean active) throws Exception {
        partitionEventHandler.thumpthump(host); // flush any heartbeats
        long refreshTimestamp = System.currentTimeMillis();
        if (!active) {
            refreshTimestamp -= topologyIsStaleAfterMillis * 2;
        }
        clusterRegistry.updateTopologies(host, Arrays.asList(
            new MiruClusterRegistry.TopologyUpdate(coord, Optional.<MiruPartitionCoordInfo>absent(), Optional.of(refreshTimestamp))));
        partitionEventHandler.thumpthump(host);
    }

    private void indexNormalActivity(MiruLocalHostedPartition localHostedPartition) throws Exception {
        localHostedPartition.index(Lists.newArrayList(
            factory.activity(1, partitionId, 0, new MiruActivity(
                tenantId, System.currentTimeMillis(), new String[0], 0,
                Collections.<String, List<String>>emptyMap(),
                Collections.<String, List<String>>emptyMap()))
        ).iterator());
    }

    private void indexBoundaryActivity(MiruLocalHostedPartition localHostedPartition) throws Exception {
        localHostedPartition.index(Lists.newArrayList(
            factory.begin(1, partitionId, tenantId, 0),
            factory.end(1, partitionId, tenantId, 0)
        ).iterator());
    }

    private <T extends Runnable> void captureRunnable(ScheduledExecutorService scheduledExecutor, final AtomicReference<T> ref, Class<T> runnableClass) {
        when(scheduledExecutor.scheduleWithFixedDelay(isA(runnableClass), anyLong(), anyLong(), any(TimeUnit.class)))
            .thenAnswer(invocation -> {
                ref.set((T) invocation.getArguments()[0]);
                return mockFuture();
            });
    }

    private <T> T waitForRef(AtomicReference<T> ref) throws InterruptedException {
        for (int i = 0; i < 100 && ref.get() == null; i++) {
            Thread.sleep(10);
        }
        if (ref.get() == null) {
            Assert.fail("Ref never caught");
        }
        return ref.get();
    }

    private ScheduledFuture<?> mockFuture() {
        ScheduledFuture<?> future = mock(ScheduledFuture.class);
        return future;
    }
}
