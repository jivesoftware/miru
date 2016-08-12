package com.jivesoftware.os.miru.bitmaps.roaring5.buffer;

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
import org.apache.commons.lang.ArrayUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
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
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, RCVSSipCursor> miruTimeIndex) throws Exception {
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
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, RCVSSipCursor> miruTimeIndex) throws Exception {
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
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, RCVSSipCursor> miruTimeIndex) throws Exception {
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

        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, RCVSSipCursor> chunkInMemoryTimeIndex = buildInMemoryTimeIndex(false);
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, RCVSSipCursor> chunkOnDiskTimeIndex = buildOnDiskTimeIndex(false);
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, RCVSSipCursor> labInMemoryTimeIndex = buildInMemoryTimeIndex(true);
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, RCVSSipCursor> labOnDiskTimeIndex = buildOnDiskTimeIndex(true);

        int[] ids = new int[timestamps.length];
        long[] monotonics = new long[timestamps.length];
        Arrays.fill(ids, -1);
        Arrays.fill(monotonics, -1);
        chunkInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        chunkOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        labInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        labOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());

        return new Object[][] {
            { new MiruBitmapsRoaringBuffer(), chunkInMemoryTimeIndex },
            { new MiruBitmapsRoaringBuffer(), chunkOnDiskTimeIndex },
            { new MiruBitmapsRoaringBuffer(), labInMemoryTimeIndex },
            { new MiruBitmapsRoaringBuffer(), labOnDiskTimeIndex }
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
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, RCVSSipCursor> chunkInMemoryTimeIndex = buildInMemoryTimeIndex(false);
        chunkInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, RCVSSipCursor> chunkOnDiskTimeIndex = buildOnDiskTimeIndex(false);
        chunkOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, RCVSSipCursor> labInMemoryTimeIndex = buildInMemoryTimeIndex(true);
        labInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, RCVSSipCursor> labOnDiskTimeIndex = buildOnDiskTimeIndex(true);
        labOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());

        return new Object[][] {
            { new MiruBitmapsRoaringBuffer(), chunkInMemoryTimeIndex },
            { new MiruBitmapsRoaringBuffer(), chunkOnDiskTimeIndex },
            { new MiruBitmapsRoaringBuffer(), labInMemoryTimeIndex },
            { new MiruBitmapsRoaringBuffer(), labOnDiskTimeIndex }
        };
    }

    @DataProvider(name = "singleEntryTimeIndexDataProvider")
    public Object[][] singleEntryTimeIndexDataProvider() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();

        final long[] timestamps = new long[] { System.currentTimeMillis() };
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, RCVSSipCursor> chunkInMemoryTimeIndex = buildInMemoryTimeIndex(false);
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, RCVSSipCursor> labInMemoryTimeIndex = buildInMemoryTimeIndex(true);
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, RCVSSipCursor> chunkOnDiskTimeIndex = buildOnDiskTimeIndex(false);
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, RCVSSipCursor> labOnDiskTimeIndex = buildOnDiskTimeIndex(true);

        int[] ids = new int[timestamps.length];
        long[] monotonics = new long[timestamps.length];
        Arrays.fill(ids, -1);
        Arrays.fill(monotonics, -1);
        chunkInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        labInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        chunkOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());
        labOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps, ids.clone(), monotonics.clone());

        return new Object[][] {
            { new MiruBitmapsRoaringBuffer(), chunkInMemoryTimeIndex },
            { new MiruBitmapsRoaringBuffer(), labInMemoryTimeIndex },
            { new MiruBitmapsRoaringBuffer(), chunkOnDiskTimeIndex },
            { new MiruBitmapsRoaringBuffer(), labOnDiskTimeIndex }
        };
    }

    private MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, RCVSSipCursor> buildInMemoryTimeIndex(boolean useLabIndexes) throws Exception {
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("logicalName"));
        return IndexTestUtil.buildInMemoryContext(numberOfChunkStores, useLabIndexes, bitmaps, coord);
    }

    private MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, RCVSSipCursor> buildOnDiskTimeIndex(boolean useLabIndexes) throws Exception {
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("logicalName"));
        return IndexTestUtil.buildOnDiskContext(numberOfChunkStores, useLabIndexes, bitmaps, coord);
    }
}
