package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.util.concurrent.MoreExecutors;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LABStats;
import com.jivesoftware.os.lab.LabHeapPressure;
import com.jivesoftware.os.lab.LabHeapPressure.FreeHeapStrategy;
import com.jivesoftware.os.lab.guts.LABHashIndexType;
import com.jivesoftware.os.lab.guts.StripingBolBufferLocks;
import com.jivesoftware.os.miru.service.locator.MiruTempDirectoryResourceLocator;
import com.jivesoftware.os.miru.service.stream.allocator.OnDiskChunkAllocator;
import java.io.File;
import java.util.Arrays;
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
    public void testAllocateAndLookup() throws Exception {
        int numberOfChunkStores = 1;
        int keepNIndexes = 4;
        int maxEntriesPerIndex = 10;

        MiruTempDirectoryResourceLocator resourceLocator = new MiruTempDirectoryResourceLocator();
        File[] labDirs = resourceLocator.getChunkDirectories(() -> new String[] { "timeId" }, "lab", -1);
        LabTimeIdIndex index = getLabTimeIdIndex(labDirs, numberOfChunkStores, keepNIndexes, maxEntriesPerIndex);

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
        LabTimeIdIndex index = getLabTimeIdIndex(labDirs, numberOfChunkStores, keepNIndexes, maxEntriesPerIndex);

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

            LabTimeIdIndex index = getLabTimeIdIndex(labDirs, numberOfChunkStores, keepNIndexes, maxEntriesPerIndex);
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
        int maxEntriesPerIndex) throws Exception {

        LABStats labStats = new LABStats();

        LabHeapPressure labHeapPressure = new LabHeapPressure(labStats, MoreExecutors.sameThreadExecutor(), "test", 1024 * 1024, 1024 * 1024 * 2,
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
        LABEnvironment[] labEnvironments = chunkAllocator.allocateTimeIdLABEnvironments(labDirs);
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
