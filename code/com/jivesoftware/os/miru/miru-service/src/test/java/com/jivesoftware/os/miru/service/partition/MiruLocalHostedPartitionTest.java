package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Charsets;
import com.google.common.collect.Interners;
import com.google.common.collect.Lists;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionCoordMetrics;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.activity.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.cluster.rcvs.MiruRCVSClusterRegistry;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.partition.MiruPartitionUnavailableException;
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaProvider;
import com.jivesoftware.os.miru.plugin.schema.MiruSchemaUnvailableException;
import com.jivesoftware.os.miru.plugin.solution.MiruRequestHandle;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.locator.MiruTempDirectoryResourceLocator;
import com.jivesoftware.os.miru.service.stream.MiruContextFactory;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReaderImpl;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALRow;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingSipWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALRow;
import com.jivesoftware.os.rcvs.api.timestamper.Timestamper;
import com.jivesoftware.os.rcvs.inmemory.RowColumnValueStoreImpl;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
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
    private MiruActivityWALReaderImpl activityWALReader;
    private MiruPartitionEventHandler partitionEventHandler;
    private ScheduledExecutorService scheduledBootstrapService;
    private ScheduledExecutorService scheduledRebuildService;
    private ScheduledExecutorService scheduledSipMigrateService;
    private ExecutorService rebuildExecutor;
    private ExecutorService rebuildIndexExecutor;
    private ExecutorService sipIndexExecutor;
    private AtomicReference<MiruLocalHostedPartition.BootstrapRunnable> bootstrapRunnable;
    private AtomicReference<MiruLocalHostedPartition.RebuildIndexRunnable> rebuildIndexRunnable;
    private AtomicReference<MiruLocalHostedPartition.SipMigrateIndexRunnable> sipMigrateIndexRunnable;
    private MiruPartitionedActivityFactory factory;
    private MiruPartitionId partitionId;
    private MiruTenantId tenantId;
    private MiruBitmapsEWAH bitmaps;
    private Timestamper timestamper;
    private AtomicLong syntheticTimestamp = new AtomicLong(System.currentTimeMillis());

    @BeforeMethod
    public void setUp() throws Exception {
        syntheticTimestamp.set(System.currentTimeMillis());
        timestamper = new Timestamper() {
            @Override
            public long get() {
                return syntheticTimestamp.incrementAndGet();
            }
        };
        tenantId = new MiruTenantId("test".getBytes(Charsets.UTF_8));
        partitionId = MiruPartitionId.of(0);
        MiruHost host = new MiruHost("localhost", 49_600);
        coord = new MiruPartitionCoord(tenantId, partitionId, host);
        defaultStorage = MiruBackingStorage.memory;

        MiruServiceConfig config = mock(MiruServiceConfig.class);
        when(config.getBitsetBufferSize()).thenReturn(32);
        when(config.getDefaultStorage()).thenReturn(defaultStorage.name());

        RowColumnValueStoreImpl<MiruTenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity> activityWAL =
            new RowColumnValueStoreImpl<>();
        RowColumnValueStoreImpl<MiruTenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity> activitySipWAL =
            new RowColumnValueStoreImpl<>();

        RowColumnValueStoreImpl<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity> readTrackingWAL =
            new RowColumnValueStoreImpl<>();
        RowColumnValueStoreImpl<MiruTenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long> readTrackingSipWAL =
            new RowColumnValueStoreImpl<>();

        schema = new MiruSchema(DefaultMiruSchemaDefinition.FIELDS);
        schemaProvider = mock(MiruSchemaProvider.class);
        when(schemaProvider.getSchema(any(MiruTenantId.class))).thenReturn(schema);

        bitmaps = new MiruBitmapsEWAH(2);

        MiruActivityInternExtern activityInternExtern = new MiruActivityInternExtern(
            Interners.<MiruIBA>newWeakInterner(),
            Interners.<MiruTermId>newWeakInterner(),
            Interners.<MiruTenantId>newStrongInterner(),
            // makes sense to share string internment as this is authz in both cases
            Interners.<String>newWeakInterner());

        contextFactory = new MiruContextFactory(schemaProvider,
            Executors.newSingleThreadExecutor(),
            new MiruReadTrackingWALReaderImpl(readTrackingWAL, readTrackingSipWAL),
            new MiruTempDirectoryResourceLocator(),
            new MiruTempDirectoryResourceLocator(),
            20,
            100,
            defaultStorage,
            activityInternExtern);
        clusterRegistry = new MiruRCVSClusterRegistry(
            timestamper,
            new RowColumnValueStoreImpl(),
            new RowColumnValueStoreImpl(),
            new RowColumnValueStoreImpl(),
            new RowColumnValueStoreImpl(),
            new RowColumnValueStoreImpl(),
            new RowColumnValueStoreImpl(),
            3,
            TimeUnit.HOURS.toMillis(1));

        activityWALReader = new MiruActivityWALReaderImpl(activityWAL, activitySipWAL);
        partitionEventHandler = new MiruPartitionEventHandler(clusterRegistry);
        factory = new MiruPartitionedActivityFactory();

        scheduledBootstrapService = mock(ScheduledExecutorService.class);
        scheduledRebuildService = mock(ScheduledExecutorService.class);
        scheduledSipMigrateService = mock(ScheduledExecutorService.class);
        rebuildExecutor = Executors.newSingleThreadExecutor();
        rebuildIndexExecutor = Executors.newSingleThreadExecutor();
        sipIndexExecutor = Executors.newSingleThreadExecutor();

        bootstrapRunnable = new AtomicReference<>();
        captureRunnable(scheduledBootstrapService, bootstrapRunnable, MiruLocalHostedPartition.BootstrapRunnable.class);

        rebuildIndexRunnable = new AtomicReference<>();
        captureRunnable(scheduledRebuildService, rebuildIndexRunnable, MiruLocalHostedPartition.RebuildIndexRunnable.class);

        sipMigrateIndexRunnable = new AtomicReference<>();
        captureRunnable(scheduledSipMigrateService, sipMigrateIndexRunnable, MiruLocalHostedPartition.SipMigrateIndexRunnable.class);
    }

    @Test
    public void testBootstrapToOnline() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = new MiruLocalHostedPartition<>(timestamper, bitmaps, coord, contextFactory,
            activityWALReader, partitionEventHandler, new HeapByteBufferFactory(), scheduledBootstrapService, scheduledRebuildService,
            scheduledSipMigrateService, rebuildExecutor, rebuildIndexExecutor, sipIndexExecutor, true, 100, 100, 10_000, 5_000, 5_000, 30_000);

        setActive(true);
        waitForRef(bootstrapRunnable).run();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.bootstrap);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);

        waitForRef(rebuildIndexRunnable).run();
        waitForRef(sipMigrateIndexRunnable).run();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.online);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);
    }

    @Test
    public void testInactiveToOffline() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = new MiruLocalHostedPartition<>(timestamper, bitmaps, coord, contextFactory,
            activityWALReader, partitionEventHandler, new HeapByteBufferFactory(), scheduledBootstrapService, scheduledRebuildService,
            scheduledSipMigrateService, rebuildExecutor, rebuildIndexExecutor, sipIndexExecutor, true, 100, 100, 10_000, 5_000, 5_000, 30_000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(rebuildIndexRunnable).run();
        waitForRef(sipMigrateIndexRunnable).run(); // online memory

        setActive(false);
        waitForRef(bootstrapRunnable).run();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);
    }

    @Test
    public void testMigrateToMemMapped() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = new MiruLocalHostedPartition<>(timestamper, bitmaps, coord, contextFactory,
            activityWALReader, partitionEventHandler, new HeapByteBufferFactory(), scheduledBootstrapService, scheduledRebuildService,
            scheduledSipMigrateService, rebuildExecutor, rebuildIndexExecutor, sipIndexExecutor, true, 100, 100, 10_000, 5_000, 5_000, 30_000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(rebuildIndexRunnable).run();
        waitForRef(sipMigrateIndexRunnable).run(); // online memory
        indexBoundaryActivity(localHostedPartition); // eligible for disk

        sipMigrateIndexRunnable.get().run(); // writers are closed, should migrate

        assertEquals(localHostedPartition.getState(), MiruPartitionState.online);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.mem_mapped);
    }

    @Test
    public void testMoveMemMappedToDisk() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = new MiruLocalHostedPartition<>(timestamper, bitmaps, coord, contextFactory,
            activityWALReader, partitionEventHandler, new HeapByteBufferFactory(), scheduledBootstrapService, scheduledRebuildService,
            scheduledSipMigrateService, rebuildExecutor, rebuildIndexExecutor, sipIndexExecutor, true, 100, 100, 10_000, 5_000, 5_000, 30_000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(rebuildIndexRunnable).run();
        waitForRef(sipMigrateIndexRunnable).run(); // online memory
        indexBoundaryActivity(localHostedPartition); // eligible for disk

        rebuildIndexRunnable.get().run(); // online mem_mapped

        localHostedPartition.setStorage(MiruBackingStorage.disk);

        assertEquals(localHostedPartition.getState(), MiruPartitionState.online); // stays online
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.disk);
    }

    @Test
    public void testMoveMemMappedToMemoryFixed() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = new MiruLocalHostedPartition<>(timestamper, bitmaps, coord, contextFactory,
            activityWALReader, partitionEventHandler, new HeapByteBufferFactory(), scheduledBootstrapService, scheduledRebuildService,
            scheduledSipMigrateService, rebuildExecutor, rebuildIndexExecutor, sipIndexExecutor, true, 100, 100, 10_000, 5_000, 5_000, 30_000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(rebuildIndexRunnable).run();
        waitForRef(sipMigrateIndexRunnable).run(); // online memory
        indexBoundaryActivity(localHostedPartition); // eligible for disk

        sipMigrateIndexRunnable.get().run(); // online mem_mapped

        localHostedPartition.setStorage(MiruBackingStorage.memory_fixed);

        assertEquals(localHostedPartition.getState(), MiruPartitionState.bootstrap); // mem_mapped -> memory triggers rebuild
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory_fixed);
    }

    @Test
    public void testMoveMemoryToMemoryFixed() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = new MiruLocalHostedPartition<>(timestamper, bitmaps, coord, contextFactory,
            activityWALReader, partitionEventHandler, new HeapByteBufferFactory(), scheduledBootstrapService, scheduledRebuildService,
            scheduledSipMigrateService, rebuildExecutor, rebuildIndexExecutor, sipIndexExecutor, true, 100, 100, 10_000, 5_000, 5_000, 30_000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(rebuildIndexRunnable).run();
        waitForRef(sipMigrateIndexRunnable).run(); // online memory

        localHostedPartition.setStorage(MiruBackingStorage.memory_fixed);

        assertEquals(localHostedPartition.getState(), MiruPartitionState.online); // stays online
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory_fixed);
    }

    @Test
    public void testMoveMemoryToMemory() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = new MiruLocalHostedPartition<>(timestamper, bitmaps, coord, contextFactory,
            activityWALReader, partitionEventHandler, new HeapByteBufferFactory(), scheduledBootstrapService, scheduledRebuildService,
            scheduledSipMigrateService, rebuildExecutor, rebuildIndexExecutor, sipIndexExecutor, true, 100, 100, 10_000, 5_000, 5_000, 30_000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(rebuildIndexRunnable).run();
        waitForRef(sipMigrateIndexRunnable).run(); // online memory

        localHostedPartition.setStorage(MiruBackingStorage.memory);

        assertEquals(localHostedPartition.getState(), MiruPartitionState.bootstrap); // memory -> memory triggers rebuild (deliberate)
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);
    }

    @Test
    public void testQueryHandleOfflineMemMappedHotDeploy() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = new MiruLocalHostedPartition<>(timestamper, bitmaps, coord, contextFactory,
            activityWALReader, partitionEventHandler, new HeapByteBufferFactory(), scheduledBootstrapService, scheduledRebuildService,
            scheduledSipMigrateService, rebuildExecutor, rebuildIndexExecutor, sipIndexExecutor, true, 100, 100, 10_000, 5_000, 5_000, 30_000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(rebuildIndexRunnable).run();
        waitForRef(sipMigrateIndexRunnable).run(); // online memory
        indexBoundaryActivity(localHostedPartition); // eligible for disk
        waitForRef(sipMigrateIndexRunnable).run(); // online memory

        setActive(false);
        waitForRef(bootstrapRunnable).run();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.mem_mapped);

        try (MiruRequestHandle queryHandle = localHostedPartition.getQueryHandle()) {
            assertEquals(queryHandle.getCoord(), coord);
            assertNotNull(queryHandle.getRequestContext()); // would throw exception if offline
        }
    }

    @Test(expectedExceptions = MiruPartitionUnavailableException.class)
    public void testQueryHandleOfflineMemoryException() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = new MiruLocalHostedPartition<>(timestamper, bitmaps, coord, contextFactory,
            activityWALReader, partitionEventHandler, new HeapByteBufferFactory(), scheduledBootstrapService, scheduledRebuildService,
            scheduledSipMigrateService, rebuildExecutor, rebuildIndexExecutor, sipIndexExecutor, true, 100, 100, 10_000, 5_000, 5_000, 30_000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(rebuildIndexRunnable).run();
        waitForRef(sipMigrateIndexRunnable).run(); // online memory

        setActive(false);
        waitForRef(bootstrapRunnable).run();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);

        try (MiruRequestHandle queryHandle = localHostedPartition.getQueryHandle()) {
            queryHandle.getRequestContext(); // throws exception
        }
    }

    @Test
    public void testRemove() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = new MiruLocalHostedPartition<>(timestamper, bitmaps, coord, contextFactory,
            activityWALReader, partitionEventHandler, new HeapByteBufferFactory(), scheduledBootstrapService, scheduledRebuildService,
            scheduledSipMigrateService, rebuildExecutor, rebuildIndexExecutor, sipIndexExecutor, true, 100, 100, 10_000, 5_000, 5_000, 30_000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(rebuildIndexRunnable).run();
        waitForRef(sipMigrateIndexRunnable).run(); // online memory

        localHostedPartition.remove();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);
    }

    @Test
    public void testWakeOnIndex_false() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = new MiruLocalHostedPartition<>(timestamper, bitmaps, coord, contextFactory,
            activityWALReader, partitionEventHandler, new HeapByteBufferFactory(), scheduledBootstrapService, scheduledRebuildService,
            scheduledSipMigrateService, rebuildExecutor, rebuildIndexExecutor, sipIndexExecutor, false, 100, 100, 10_000, 5_000, 5_000, 30_000);

        indexNormalActivity(localHostedPartition);
        waitForRef(bootstrapRunnable).run(); // stays offline

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);
    }

    @Test
    public void testWakeOnIndex_true() throws Exception {
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = new MiruLocalHostedPartition<>(timestamper, bitmaps, coord, contextFactory,
            activityWALReader, partitionEventHandler, new HeapByteBufferFactory(), scheduledBootstrapService, scheduledRebuildService,
            scheduledSipMigrateService, rebuildExecutor, rebuildIndexExecutor, sipIndexExecutor, true, 100, 100, 10_000, 5_000, 5_000, 30_000);

        indexNormalActivity(localHostedPartition);
        waitForRef(bootstrapRunnable).run(); // bootstrap

        assertEquals(localHostedPartition.getState(), MiruPartitionState.bootstrap);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);
    }

    @Test
    public void testSchemaNotRegistered_checkActive() throws Exception {
        when(schemaProvider.getSchema(any(MiruTenantId.class))).thenThrow(new MiruSchemaUnvailableException("test"));
        MiruLocalHostedPartition<EWAHCompressedBitmap> localHostedPartition = new MiruLocalHostedPartition<>(timestamper, bitmaps, coord, contextFactory,
            activityWALReader, partitionEventHandler, new HeapByteBufferFactory(), scheduledBootstrapService, scheduledRebuildService,
            scheduledSipMigrateService, rebuildExecutor, rebuildIndexExecutor, sipIndexExecutor, false, 100, 100, 10_000, 5_000, 5_000, 30_000);

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), defaultStorage);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // no effect

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), defaultStorage);
    }

    private void setActive(boolean active) throws Exception {
        clusterRegistry.refreshTopology(coord, new MiruPartitionCoordMetrics(-1, -1), syntheticTimestamp.incrementAndGet());
        if (!active) {
            syntheticTimestamp.addAndGet(TimeUnit.HOURS.toMillis(1) + 1000);
        }
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
            .thenAnswer(new Answer<ScheduledFuture<?>>() {
                @Override
                @SuppressWarnings("unchecked")
                public ScheduledFuture<?> answer(InvocationOnMock invocation) throws Throwable {
                    ref.set((T) invocation.getArguments()[0]);
                    return mockFuture();
                }
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
