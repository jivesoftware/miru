package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.util.concurrent.MoreExecutors;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LABStats;
import com.jivesoftware.os.lab.LabHeapPressure;
import com.jivesoftware.os.lab.LabHeapPressure.FreeHeapStrategy;
import com.jivesoftware.os.lab.api.JournalStream;
import com.jivesoftware.os.lab.guts.LABHashIndexType;
import com.jivesoftware.os.lab.guts.StripingBolBufferLocks;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.service.locator.DiskIdentifierPartResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruTempDirectoryResourceLocator;
import com.jivesoftware.os.miru.service.stream.allocator.OnDiskChunkAllocator;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.math.LongRange;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class LabTimeIdIndexTest {

    @Test
    public void testConcurrency() throws Exception {
        int numberOfChunkStores = 1;
        int keepNIndexes = 4;
        int maxEntriesPerIndex = 10;

        ExecutorService timeIdExecutorService = Executors.newFixedThreadPool(8);
        File[] resourcePaths = { Files.createTempDir() };
        DiskIdentifierPartResourceLocator resourceLocator = new DiskIdentifierPartResourceLocator(resourcePaths, -1L, -1L);
        File[] labDirs = resourceLocator.getChunkDirectories(() -> new String[] { "timeId" }, "lab", -1);
        LabTimeIdIndex timeIdIndex = getLabTimeIdIndex(labDirs, numberOfChunkStores, keepNIndexes, maxEntriesPerIndex, timeIdExecutorService, null);

        int concurrencyLevel = 8;
        int entriesPerBatch = 10;

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong[] payloads = new AtomicLong[concurrencyLevel];

        System.out.println("Running...");
        ExecutorService executorService = Executors.newFixedThreadPool(concurrencyLevel);
        List<Future<?>> futures = Lists.newArrayList();
        for (int i = 0; i < concurrencyLevel; i++) {
            int index = i;
            payloads[i] = new AtomicLong(0);
            MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(i), new MiruHost("localhost"));
            LabTimeIdIndex timeIdIndex1 = timeIdIndex;
            long version = (long) i;
            futures.add(executorService.submit(() -> {
                int count = 0;
                while (running.get()) {
                    if (count % 1_000 == 0) {
                        System.out.println("Thread:" + index + " count:" + count);
                    }
                    count++;

                    payloads[index].addAndGet(entriesPerBatch);
                    long[] timestamps = new LongRange(count, count + entriesPerBatch - 1).toArray();
                    int[] ids = new int[entriesPerBatch];
                    long[] monotonics = new long[entriesPerBatch];
                    timeIdIndex1.allocate(coord, version, timestamps, ids, monotonics, -1, -1L);
                    Thread.yield();
                }
                return true;
            }));
        }

        Thread.sleep(5_000L);
        running.set(false);

        System.out.println("Stopping...");
        for (Future<?> future : futures) {
            future.get();
        }

        System.out.println("Closing...");
        timeIdIndex.close();
        executorService.shutdownNow();

        System.out.println("Reopening...");
        AtomicLong[] journalCount = new AtomicLong[concurrencyLevel];
        for (int i = 0; i < concurrencyLevel; i++) {
            journalCount[i] = new AtomicLong(0);
        }
        timeIdIndex = getLabTimeIdIndex(labDirs, numberOfChunkStores, keepNIndexes, maxEntriesPerIndex, timeIdExecutorService, null);

        for (int i = 0; i < concurrencyLevel; i++) {
            long version = (long) i;
            for (int j = 0; j < payloads[i].get(); j += entriesPerBatch) {
                long[] timestamps = new LongRange(j, j + entriesPerBatch - 1).toArray();
                int[] ids = new int[entriesPerBatch];
                long[] monotonics = new long[entriesPerBatch];
                timeIdIndex.lookup(version, timestamps, ids, monotonics);
                for (int k = 0; k < entriesPerBatch; k++) {
                    if (ids[k] >= 0) {
                        journalCount[i].incrementAndGet();
                    }
                }
            }
            assertEquals(payloads[i].get(), journalCount[i].get(), "Index:" + i);
        }
        timeIdExecutorService.shutdownNow();
    }

    @Test
    public void testAllocateAndLookup() throws Exception {
        int numberOfChunkStores = 1;
        int keepNIndexes = 4;
        int maxEntriesPerIndex = 10;

        MiruTempDirectoryResourceLocator resourceLocator = new MiruTempDirectoryResourceLocator();
        File[] labDirs = resourceLocator.getChunkDirectories(() -> new String[] { "timeId" }, "lab", -1);
        LabTimeIdIndex index = getLabTimeIdIndex(labDirs, numberOfChunkStores, keepNIndexes, maxEntriesPerIndex, MoreExecutors.sameThreadExecutor(), null);

        for (long version : new LongRange(1000, 2000).toArray()) {
            long[] timestamps = { 1L, 2L, 3L, 4L };
            int[] ids = { -1, -1, -1, -1 };
            long[] monotonics = { -1, -1, -1, -1 };
            index.lookup(version, timestamps, ids, monotonics);
            assertCount(keepNIndexes, maxEntriesPerIndex, index);
            for (int i = 0; i < timestamps.length; i++) {
                assertEquals(ids[i], -1);
                assertEquals(monotonics[i], -1);
            }

            index.allocate(null, version, timestamps, ids, monotonics, -1, -1);
            assertCount(keepNIndexes, maxEntriesPerIndex, index);
            for (int i = 0; i < timestamps.length; i++) {
                assertEquals(ids[i], i);
                assertEquals(monotonics[i], i + 1);
            }

            // minimum history of 40 entries divided by 5 entries per version equals at least 8 versions with history
            for (int v = 0; v < 8; v++) {
                long checkVersion = version - v;
                if (checkVersion < 1000) {
                    break;
                }
                Arrays.fill(ids, -1);
                Arrays.fill(monotonics, -1);
                index.lookup(checkVersion, timestamps, ids, monotonics);
                assertCount(keepNIndexes, maxEntriesPerIndex, index);
                for (int i = 0; i < timestamps.length; i++) {
                    assertEquals(ids[i], i);
                    assertEquals(monotonics[i], i + 1);
                }
            }
        }
    }

    @Test
    public void testDuplicatesInBatch() throws Exception {
        int numberOfChunkStores = 1;
        int keepNIndexes = 4;
        int maxEntriesPerIndex = 10;

        MiruTempDirectoryResourceLocator resourceLocator = new MiruTempDirectoryResourceLocator();
        File[] labDirs = resourceLocator.getChunkDirectories(() -> new String[] { "timeId" }, "lab", -1);
        LabTimeIdIndex index = getLabTimeIdIndex(labDirs, numberOfChunkStores, keepNIndexes, maxEntriesPerIndex, MoreExecutors.sameThreadExecutor(), null);

        long version = 0L;
        long[] timestamps = { 1L, 2L, 3L, 4L, 1L, 2L, 3L, 4L, 1L, 2L, 3L, 4L };
        int[] ids = { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 };
        long[] monotonics = { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 };
        index.lookup(version, timestamps, ids, monotonics);
        assertCount(keepNIndexes, maxEntriesPerIndex, index);
        for (int i = 0; i < timestamps.length; i++) {
            assertEquals(ids[i], -1);
            assertEquals(monotonics[i], -1);
        }

        index.allocate(null, version, timestamps, ids, monotonics, -1, -1);
        assertCount(keepNIndexes, maxEntriesPerIndex, index);
        for (int i = 0; i < timestamps.length; i++) {
            assertEquals(ids[i], i);
            assertEquals(monotonics[i], i + 1);
        }

    }

    @Test
    public void testReopenConsistency() throws Exception {
        int numberOfChunkStores = 1;
        int keepNIndexes = 4;
        int maxEntriesPerIndex = 10;
        long version = 0L;

        MiruTempDirectoryResourceLocator resourceLocator = new MiruTempDirectoryResourceLocator();
        File[] labDirs = resourceLocator.getChunkDirectories(() -> new String[] { "timeId" }, "lab", -1);

        AtomicLong ts = new AtomicLong(-1L);
        AtomicLong largestTimestamp = new AtomicLong(-1);
        AtomicInteger lastId = new AtomicInteger(-1);
        int batchSize = 100;
        for (int i = 0; i < 10; i++) {
            System.out.println("timeId reopen consistency run " + i);
            long[] timestamps = new long[batchSize];
            for (int j = 0; j < timestamps.length; j++) {
                timestamps[j] = ts.incrementAndGet();
            }

            int[] ids = new int[timestamps.length];
            Arrays.fill(ids, -1);
            long[] monotonics = new long[timestamps.length];
            Arrays.fill(monotonics, -1);

            LabTimeIdIndex index = getLabTimeIdIndex(labDirs, numberOfChunkStores, keepNIndexes, maxEntriesPerIndex, MoreExecutors.sameThreadExecutor(), null);
            index.allocate(null, version, timestamps, ids, monotonics, -1, -1);
            index.close();

            assertEquals(ids[0], lastId.get() + 1);
            assertEquals(ids[ids.length - 1], lastId.get() + batchSize);
            assertEquals(timestamps[0], largestTimestamp.get() + 1);
            assertEquals(timestamps[timestamps.length - 1], largestTimestamp.get() + batchSize);
            lastId.set(ids[ids.length - 1]);
            largestTimestamp.set(timestamps[timestamps.length - 1]);
        }
    }

    private LabTimeIdIndex getLabTimeIdIndex(File[] labDirs,
        int numberOfChunkStores,
        int keepNIndexes,
        int maxEntriesPerIndex,
        ExecutorService executorService,
        JournalStream journalStream) throws Exception {

        LABStats labStats = new LABStats();

        LabHeapPressure labHeapPressure = new LabHeapPressure(labStats, executorService, "test", 1024 * 1024, 1024 * 1024 * 2,
            new AtomicLong(),
            FreeHeapStrategy.mostBytesFirst);
        OnDiskChunkAllocator chunkAllocator = new OnDiskChunkAllocator(
            null, // shouldn't need a resource allocator
            new HeapByteBufferFactory(),
            numberOfChunkStores,
            100,
            1_000,
            new LABStats[] { labStats },
            new LabHeapPressure[] { labHeapPressure },
            labHeapPressure,
            10 * 1024 * 1024,
            1000,
            10 * 1024 * 1024,
            10 * 1024 * 1024,
            true,
            LABEnvironment.buildLeapsCache(1_000_000, 10),
            new StripingBolBufferLocks(2048));

        for (File labDir : labDirs) {
            labDir.mkdirs();
        }
        LABEnvironment[] labEnvironments = chunkAllocator.allocateTimeIdLABEnvironments(labDirs, journalStream);
        LabTimeIdIndex[] indexes = new LabTimeIdIndexInitializer().initialize(keepNIndexes,
            maxEntriesPerIndex,
            1024 * 1024,
            LABHashIndexType.cuckoo,
            2d,
            true,
            false,
            false,
            labEnvironments);

        assertEquals(indexes.length, numberOfChunkStores);
        return indexes[0];
    }

    private void assertCount(int keepNIndexes, int maxEntriesPerIndex, LabTimeIdIndex index) throws Exception {
        assertTrue(index.count() < (keepNIndexes * (maxEntriesPerIndex + 5))); // max overflow of 4 entries plus version = 5
    }
}
