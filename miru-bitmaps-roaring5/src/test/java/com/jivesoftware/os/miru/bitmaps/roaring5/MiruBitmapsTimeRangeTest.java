package com.jivesoftware.os.miru.bitmaps.roaring5;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.service.IndexTestUtil;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import java.util.Arrays;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 *
 */
public class MiruBitmapsTimeRangeTest {

    private final int numberOfChunkStores = 4;

    @Test(dataProvider = "evenTimeIndexDataProvider")
    public <BM extends IBM, IBM> void testBuildEvenTimeRangeMask(MiruBitmaps<BM, IBM> bitmaps,
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> miruTimeIndex) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        final int size = (64 * 3) + 1;
        for (int lower = 0; lower <= size / 2; lower++) {
            int upper = size - 1 - lower;

            IBM bitmap = bitmaps.buildTimeRangeMask(miruTimeIndex.timeIndex, lower, upper, stackBuffer);
            if (lower == upper) {
                // the lower and upper are the same so there should be nothing left
                assertExpectedNumberOfConsecutiveBitsStartingFromN(bitmaps, bitmap, -1, 0);
            } else {
                assertExpectedNumberOfConsecutiveBitsStartingFromN(bitmaps, bitmap, lower + 1, size - 1 - 2 * lower);
            }
        }
    }

    @Test(dataProvider = "oddTimeIndexDataProvider")
    public <BM extends IBM, IBM> void testBuildOddTimeRangeMask(MiruBitmaps<BM, IBM> bitmaps,
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> miruTimeIndex) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        final int size = 64 * 3;
        for (int lower = 0; lower < size / 2; lower++) {
            int upper = size - 1 - lower;

            IBM bitmap = bitmaps.buildTimeRangeMask(miruTimeIndex.timeIndex, lower, upper, stackBuffer);
            if (lower == upper) {
                fail();
            } else {
                assertExpectedNumberOfConsecutiveBitsStartingFromN(bitmaps, bitmap, lower + 1, size - 1 - 2 * lower);
            }
        }
    }

    @Test(dataProvider = "singleEntryTimeIndexDataProvider")
    public <BM extends IBM, IBM> void testSingleBitTimeRange(MiruBitmaps<BM, IBM> bitmaps,
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> miruTimeIndex) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        IBM bitmap = bitmaps.buildTimeRangeMask(miruTimeIndex.timeIndex, 0, Long.MAX_VALUE, stackBuffer);

        assertExpectedNumberOfConsecutiveBitsStartingFromN(bitmaps, bitmap, 0, 1);
    }

    private <BM extends IBM, IBM> void assertExpectedNumberOfConsecutiveBitsStartingFromN(MiruBitmaps<BM, IBM> bitmaps, IBM bitmap, int expectedStartingFrom,
        int expectedCardinality) {
        int last = -1;
        int cardinality = 0;
        int startingFrom = -1;
        MiruIntIterator iter = bitmaps.intIterator(bitmap);
        while (iter.hasNext()) {
            int current = iter.next();
            if (last < 0) {
                startingFrom = current;
            } else if (last >= 0) {
                assertEquals(current - last, 1);
            }
            last = current;
            cardinality++;
        }
        assertEquals(startingFrom, expectedStartingFrom);
        assertEquals(cardinality, expectedCardinality);
    }

    @DataProvider(name = "evenTimeIndexDataProvider")
    public Object[][] evenTimeIndexDataProvider() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        //MiruSchema schema = new Builder("test", 1).build();

        final int size = (64 * 3) + 1;
        final long[] timestamps = new long[size];
        for (int i = 0; i < size; i++) {
            timestamps[i] = i;
        }

        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> chunkInMemoryTimeIndex = buildInMemoryTimeIndex(false);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> chunkOnDiskTimeIndex = buildOnDiskTimeIndex(false);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> labInMemoryTimeIndex = buildInMemoryTimeIndex(true);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> labOnDiskTimeIndex = buildOnDiskTimeIndex(true);

        int[] ids = new int[timestamps.length];
        long[] monotonics = new long[timestamps.length];
        Arrays.fill(ids, -1);
        Arrays.fill(monotonics, -1);
        chunkInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        chunkOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        labInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        labOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());

        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> chunkInMemoryTimeIndexMerged = buildInMemoryTimeIndex(false);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> chunkOnDiskTimeIndexMerged = buildOnDiskTimeIndex(false);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> labInMemoryTimeIndexMerged = buildInMemoryTimeIndex(true);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> labOnDiskTimeIndexMerged = buildOnDiskTimeIndex(true);

        chunkInMemoryTimeIndexMerged.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        chunkOnDiskTimeIndexMerged.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        labInMemoryTimeIndexMerged.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        labOnDiskTimeIndexMerged.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());

        return new Object[][] {
            { new MiruBitmapsRoaring(), chunkInMemoryTimeIndex },
            { new MiruBitmapsRoaring(), chunkOnDiskTimeIndex },
            { new MiruBitmapsRoaring(), labInMemoryTimeIndex },
            { new MiruBitmapsRoaring(), labOnDiskTimeIndex }
        };
    }

    @DataProvider(name = "oddTimeIndexDataProvider")
    public Object[][] oddTimeIndexDataProvider() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();

        final int size = 64 * 3;
        final long[] timestamps = new long[size];
        for (int i = 0; i < size; i++) {
            timestamps[i] = i;
        }

        int[] ids = new int[timestamps.length];
        long[] monotonics = new long[timestamps.length];
        Arrays.fill(ids, -1);
        Arrays.fill(monotonics, -1);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> chunkInMemoryTimeIndex = buildInMemoryTimeIndex(false);
        chunkInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> chunkOnDiskTimeIndex = buildOnDiskTimeIndex(false);
        chunkOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> labInMemoryTimeIndex = buildInMemoryTimeIndex(true);
        labInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> labOnDiskTimeIndex = buildOnDiskTimeIndex(true);
        labOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());

        return new Object[][] {
            { new MiruBitmapsRoaring(), chunkInMemoryTimeIndex },
            { new MiruBitmapsRoaring(), chunkOnDiskTimeIndex },
            { new MiruBitmapsRoaring(), labInMemoryTimeIndex },
            { new MiruBitmapsRoaring(), labOnDiskTimeIndex }
        };
    }

    @DataProvider(name = "singleEntryTimeIndexDataProvider")
    public Object[][] singleEntryTimeIndexDataProvider() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();

        final long[] timestamps = new long[] { System.currentTimeMillis() };
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> chunkInMemoryTimeIndex = buildInMemoryTimeIndex(false);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> labInMemoryTimeIndex = buildInMemoryTimeIndex(true);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> chunkOnDiskTimeIndex = buildOnDiskTimeIndex(false);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> labOnDiskTimeIndex = buildOnDiskTimeIndex(true);

        int[] ids = new int[timestamps.length];
        long[] monotonics = new long[timestamps.length];
        Arrays.fill(ids, -1);
        Arrays.fill(monotonics, -1);
        chunkInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        labInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        chunkOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        labOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());

        return new Object[][] {
            { new MiruBitmapsRoaring(), chunkInMemoryTimeIndex },
            { new MiruBitmapsRoaring(), labInMemoryTimeIndex },
            { new MiruBitmapsRoaring(), chunkOnDiskTimeIndex },
            { new MiruBitmapsRoaring(), labOnDiskTimeIndex }
        };
    }

    private MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> buildInMemoryTimeIndex(boolean useLabIndexes) throws Exception {
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("logicalName"));
        return IndexTestUtil.buildInMemoryContext(numberOfChunkStores, useLabIndexes, true, bitmaps, coord);
    }

    private MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> buildOnDiskTimeIndex(boolean useLabIndexes) throws Exception {
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("logicalName"));
        return IndexTestUtil.buildOnDiskContext(numberOfChunkStores, useLabIndexes, true, bitmaps, coord);
    }
}
