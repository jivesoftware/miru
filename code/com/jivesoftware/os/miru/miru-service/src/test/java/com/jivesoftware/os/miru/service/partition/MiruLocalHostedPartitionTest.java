package com.jivesoftware.os.miru.service.partition;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.jive.utils.row.column.value.store.inmemory.RowColumnValueStoreImpl;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.MiruPartitionState;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionedActivityFactory;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.cluster.memory.MiruInMemoryClusterRegistry;
import com.jivesoftware.os.miru.service.MiruServiceConfig;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.service.schema.MiruSchema;
import com.jivesoftware.os.miru.service.stream.MiruStreamFactory;
import com.jivesoftware.os.miru.service.stream.locator.MiruTempDirectoryResourceLocator;
import com.jivesoftware.os.miru.wal.activity.MiruActivityWALReaderImpl;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivitySipWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALColumnKey;
import com.jivesoftware.os.miru.wal.activity.hbase.MiruActivityWALRow;
import com.jivesoftware.os.miru.wal.readtracking.MiruReadTrackingWALReaderImpl;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingSipWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALColumnKey;
import com.jivesoftware.os.miru.wal.readtracking.hbase.MiruReadTrackingWALRow;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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

    private MiruStreamFactory streamFactory;
    private MiruSchema schema;
    private MiruInMemoryClusterRegistry clusterRegistry;
    private MiruPartitionCoord coord;
    private MiruActivityWALReaderImpl activityWALReader;
    private MiruPartitionEventHandler partitionEventHandler;
    private ScheduledExecutorService scheduledExecutorService;
    private AtomicReference<MiruLocalHostedPartition.BootstrapRunnable> bootstrapRunnable;
    private AtomicReference<MiruLocalHostedPartition.ManageIndexRunnable> manageIndexRunnable;
    private MiruPartitionedActivityFactory factory;
    private MiruPartitionId partitionId;
    private MiruTenantId tenantId;

    @BeforeMethod
    public void setUp() throws Exception {
        tenantId = new MiruTenantId("test".getBytes(Charsets.UTF_8));
        partitionId = MiruPartitionId.of(0);
        MiruHost host = new MiruHost("localhost", 49600);
        coord = new MiruPartitionCoord(tenantId, partitionId, host);

        MiruServiceConfig config = mock(MiruServiceConfig.class);
        when(config.getBitsetBufferSize()).thenReturn(32);
        when(config.getDefaultStorage()).thenReturn(MiruBackingStorage.memory.name());

        RowColumnValueStoreImpl<TenantId, MiruActivityWALRow, MiruActivityWALColumnKey, MiruPartitionedActivity> activityWAL =
            new RowColumnValueStoreImpl<>();
        RowColumnValueStoreImpl<TenantId, MiruActivityWALRow, MiruActivitySipWALColumnKey, MiruPartitionedActivity> activitySipWAL =
            new RowColumnValueStoreImpl<>();

        RowColumnValueStoreImpl<TenantId, MiruReadTrackingWALRow, MiruReadTrackingWALColumnKey, MiruPartitionedActivity> readTrackingWAL =
            new RowColumnValueStoreImpl<>();
        RowColumnValueStoreImpl<TenantId, MiruReadTrackingWALRow, MiruReadTrackingSipWALColumnKey, Long> readTrackingSipWAL =
            new RowColumnValueStoreImpl<>();

        schema = new MiruSchema(DefaultMiruSchemaDefinition.SCHEMA);

        streamFactory = new MiruStreamFactory(new MiruBitmapsEWAH(2), // TODO consider feed wth data provider
            schema,
            Executors.newSingleThreadExecutor(),
            new MiruReadTrackingWALReaderImpl(readTrackingWAL, readTrackingSipWAL),
            new MiruTempDirectoryResourceLocator(),
            new MiruTempDirectoryResourceLocator(),
            32,
            20,
            MiruBackingStorage.memory);
        clusterRegistry = new MiruInMemoryClusterRegistry();

        activityWALReader = new MiruActivityWALReaderImpl(activityWAL, activitySipWAL);
        partitionEventHandler = new MiruPartitionEventHandler(clusterRegistry);
        factory = new MiruPartitionedActivityFactory();

        scheduledExecutorService = mock(ScheduledExecutorService.class);

        bootstrapRunnable = new AtomicReference<>();
        captureRunnable(bootstrapRunnable, MiruLocalHostedPartition.BootstrapRunnable.class);

        manageIndexRunnable = new AtomicReference<>();
        captureRunnable(manageIndexRunnable, MiruLocalHostedPartition.ManageIndexRunnable.class);
    }

    @Test
    public void testBootstrapToOnline() throws Exception {
        MiruLocalHostedPartition localHostedPartition = new MiruLocalHostedPartition(
            coord, streamFactory, activityWALReader, partitionEventHandler, scheduledExecutorService, 100, 5000, 5000);

        setActive(true);
        waitForRef(bootstrapRunnable).run();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.bootstrap);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);

        waitForRef(manageIndexRunnable).run();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.online);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);
    }

    @Test
    public void testInactiveToOffline() throws Exception {
        MiruLocalHostedPartition localHostedPartition = new MiruLocalHostedPartition(
            coord, streamFactory, activityWALReader, partitionEventHandler, scheduledExecutorService, 100, 5000, 5000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(manageIndexRunnable).run(); // online memory

        setActive(false);
        waitForRef(bootstrapRunnable).run();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);
    }

    @Test
    public void testMigrateToMemMapped() throws Exception {
        MiruLocalHostedPartition localHostedPartition = new MiruLocalHostedPartition(
            coord, streamFactory, activityWALReader, partitionEventHandler, scheduledExecutorService, 100, 5000, 5000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(manageIndexRunnable).run(); // online memory
        indexBoundaryActivity(localHostedPartition); // eligible for disk

        manageIndexRunnable.get().run(); // writers are closed, should migrate

        assertEquals(localHostedPartition.getState(), MiruPartitionState.online);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.mem_mapped);
    }

    @Test
    public void testMoveMemMappedToDisk() throws Exception {
        MiruLocalHostedPartition localHostedPartition = new MiruLocalHostedPartition(
            coord, streamFactory, activityWALReader, partitionEventHandler, scheduledExecutorService, 100, 5000, 5000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(manageIndexRunnable).run(); // online memory
        indexBoundaryActivity(localHostedPartition); // eligible for disk

        manageIndexRunnable.get().run(); // online mem_mapped

        localHostedPartition.setStorage(MiruBackingStorage.disk);

        assertEquals(localHostedPartition.getState(), MiruPartitionState.online); // stays online
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.disk);
    }

    @Test
    public void testMoveMemMappedToMemoryFixed() throws Exception {
        MiruLocalHostedPartition localHostedPartition = new MiruLocalHostedPartition(
            coord, streamFactory, activityWALReader, partitionEventHandler, scheduledExecutorService, 100, 5000, 5000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(manageIndexRunnable).run(); // online memory
        indexBoundaryActivity(localHostedPartition); // eligible for disk

        manageIndexRunnable.get().run(); // online mem_mapped

        localHostedPartition.setStorage(MiruBackingStorage.memory_fixed);

        assertEquals(localHostedPartition.getState(), MiruPartitionState.bootstrap); // mem_mapped -> memory triggers rebuild
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory_fixed);
    }

    @Test
    public void testMoveMemoryToMemoryFixed() throws Exception {
        MiruLocalHostedPartition localHostedPartition = new MiruLocalHostedPartition(
            coord, streamFactory, activityWALReader, partitionEventHandler, scheduledExecutorService, 100, 5000, 5000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(manageIndexRunnable).run(); // online memory

        localHostedPartition.setStorage(MiruBackingStorage.memory_fixed);

        assertEquals(localHostedPartition.getState(), MiruPartitionState.online); // stays online
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory_fixed);
    }

    @Test
    public void testMoveMemoryToMemory() throws Exception {
        MiruLocalHostedPartition localHostedPartition = new MiruLocalHostedPartition(
            coord, streamFactory, activityWALReader, partitionEventHandler, scheduledExecutorService, 100, 5000, 5000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(manageIndexRunnable).run(); // online memory

        localHostedPartition.setStorage(MiruBackingStorage.memory);

        assertEquals(localHostedPartition.getState(), MiruPartitionState.bootstrap); // memory -> memory triggers rebuild (deliberate)
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);
    }

    @Test
    public void testQueryHandleOfflineMemMappedHotDeploy() throws Exception {
        MiruLocalHostedPartition localHostedPartition = new MiruLocalHostedPartition(
            coord, streamFactory, activityWALReader, partitionEventHandler, scheduledExecutorService, 100, 5000, 5000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(manageIndexRunnable).run(); // online memory
        indexBoundaryActivity(localHostedPartition); // eligible for disk
        waitForRef(manageIndexRunnable).run(); // online mem_mapped

        setActive(false);
        waitForRef(bootstrapRunnable).run();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.mem_mapped);

        try (MiruQueryHandle queryHandle = localHostedPartition.getQueryHandle()) {
            assertEquals(queryHandle.getPartitionId(), partitionId);
            assertNotNull(queryHandle.getQueryStream()); // would throw exception if offline
        }
    }

    @Test(expectedExceptions = MiruPartitionUnavailableException.class)
    public void testQueryHandleOfflineMemoryException() throws Exception {
        MiruLocalHostedPartition localHostedPartition = new MiruLocalHostedPartition(
            coord, streamFactory, activityWALReader, partitionEventHandler, scheduledExecutorService, 100, 5000, 5000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(manageIndexRunnable).run(); // online memory

        setActive(false);
        waitForRef(bootstrapRunnable).run();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);

        try (MiruQueryHandle queryHandle = localHostedPartition.getQueryHandle()) {
            queryHandle.getQueryStream(); // throws exception
        }
    }

    @Test
    public void testRemove() throws Exception {
        MiruLocalHostedPartition localHostedPartition = new MiruLocalHostedPartition(
            coord, streamFactory, activityWALReader, partitionEventHandler, scheduledExecutorService, 100, 5000, 5000);

        setActive(true);
        waitForRef(bootstrapRunnable).run(); // bootstrap
        waitForRef(manageIndexRunnable).run(); // online memory

        localHostedPartition.remove();

        assertEquals(localHostedPartition.getState(), MiruPartitionState.offline);
        assertEquals(localHostedPartition.getStorage(), MiruBackingStorage.memory);
    }

    private void setActive(boolean active) throws Exception {
        clusterRegistry.refreshTopology(coord, null, active ? System.currentTimeMillis() : 0);
    }

    private void indexBoundaryActivity(MiruLocalHostedPartition localHostedPartition) throws Exception {
        localHostedPartition.index(Lists.newArrayList(
            factory.begin(1, partitionId, tenantId, 0),
            factory.end(1, partitionId, tenantId, 0)
        ).iterator());
    }

    private <T extends Runnable> void captureRunnable(final AtomicReference<T> ref, Class<T> runnableClass) {
        when(scheduledExecutorService.scheduleWithFixedDelay(isA(runnableClass), anyLong(), anyLong(), any(TimeUnit.class)))
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