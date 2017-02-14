package com.jivesoftware.os.miru.service.index.lab;

import com.google.common.util.concurrent.MoreExecutors;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LABStats;
import com.jivesoftware.os.lab.LabHeapPressure;
import com.jivesoftware.os.lab.LabHeapPressure.FreeHeapStrategy;
import com.jivesoftware.os.lab.guts.StripingBolBufferLocks;
import com.jivesoftware.os.miru.service.locator.MiruTempDirectoryResourceLocator;
import com.jivesoftware.os.miru.service.stream.allocator.OnDiskChunkAllocator;
import java.util.Arrays;
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
        LabTimeIdIndex index = getLabTimeIdIndex(numberOfChunkStores, keepNIndexes, maxEntriesPerIndex);

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

            index.allocate(version, timestamps, ids, monotonics, -1, -1);
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
        LabTimeIdIndex index = getLabTimeIdIndex(numberOfChunkStores, keepNIndexes, maxEntriesPerIndex);

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

        index.allocate(version, timestamps, ids, monotonics, -1, -1);
        assertCount(keepNIndexes, maxEntriesPerIndex, index);
        for (int i = 0; i < timestamps.length; i++) {
            assertEquals(ids[i], i);
            assertEquals(monotonics[i], i + 1);
        }

    }

    private LabTimeIdIndex getLabTimeIdIndex(int numberOfChunkStores, int keepNIndexes, int maxEntriesPerIndex) throws Exception {
        MiruTempDirectoryResourceLocator resourceLocator = new MiruTempDirectoryResourceLocator();
        LABStats labStats = new LABStats();

        LabHeapPressure labHeapPressure = new LabHeapPressure(labStats, MoreExecutors.sameThreadExecutor(), "test", 1024 * 1024, 1024 * 1024 * 2,
            new AtomicLong(),
            FreeHeapStrategy.mostBytesFirst);
        OnDiskChunkAllocator chunkAllocator = new OnDiskChunkAllocator(new MiruTempDirectoryResourceLocator(),
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

        LabTimeIdIndex[] indexes = new LabTimeIdIndexInitializer().initialize(keepNIndexes, maxEntriesPerIndex, 1024 * 1024, 1d, false, resourceLocator,
            chunkAllocator);

        assertEquals(indexes.length, numberOfChunkStores);
        return indexes[0];
    }

    private void assertCount(int keepNIndexes, int maxEntriesPerIndex, LabTimeIdIndex index) throws Exception {
        assertTrue(index.count() < (keepNIndexes * (maxEntriesPerIndex + 5))); // max overflow of 4 entries plus version = 5
    }
}
