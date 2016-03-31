package com.jivesoftware.os.miru.bitmaps.roaring5;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema.Builder;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.IndexTestUtil;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaTimeIndex;
import com.jivesoftware.os.miru.service.stream.MiruContext;
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
        MiruSchema schema = new Builder("test", 1).build();

        final int size = (64 * 3) + 1;
        final long[] timestamps = new long[size];
        for (int i = 0; i < size; i++) {
            timestamps[i] = i;
        }

        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> chunkInMemoryTimeIndex = buildInMemoryTimeIndex(false);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> chunkOnDiskTimeIndex = buildOnDiskTimeIndex(false);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> labInMemoryTimeIndex = buildInMemoryTimeIndex(true);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> labOnDiskTimeIndex = buildOnDiskTimeIndex(true);

        chunkInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps);
        chunkOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps);
        labInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps);
        labOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps);

        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> chunkInMemoryTimeIndexMerged = buildInMemoryTimeIndex(false);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> chunkOnDiskTimeIndexMerged = buildOnDiskTimeIndex(false);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> labInMemoryTimeIndexMerged = buildInMemoryTimeIndex(true);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> labOnDiskTimeIndexMerged = buildOnDiskTimeIndex(true);

        chunkInMemoryTimeIndexMerged.timeIndex.nextId(stackBuffer, timestamps);
        chunkOnDiskTimeIndexMerged.timeIndex.nextId(stackBuffer, timestamps);
        labInMemoryTimeIndexMerged.timeIndex.nextId(stackBuffer, timestamps);
        labOnDiskTimeIndexMerged.timeIndex.nextId(stackBuffer, timestamps);

        ((MiruDeltaTimeIndex) chunkInMemoryTimeIndexMerged.timeIndex).merge(schema, stackBuffer);
        chunkInMemoryTimeIndexMerged.commitable.commit();
        ((MiruDeltaTimeIndex) chunkOnDiskTimeIndexMerged.timeIndex).merge(schema, stackBuffer);
        chunkOnDiskTimeIndexMerged.commitable.commit();
        ((MiruDeltaTimeIndex) labInMemoryTimeIndexMerged.timeIndex).merge(schema, stackBuffer);
        labInMemoryTimeIndexMerged.commitable.commit();
        ((MiruDeltaTimeIndex) labOnDiskTimeIndexMerged.timeIndex).merge(schema, stackBuffer);
        labOnDiskTimeIndexMerged.commitable.commit();

        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> chunkInMemoryTimeIndexPartiallyMerged = buildInMemoryTimeIndex(false);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> chunkOnDiskTimeIndexPartiallyMerged = buildOnDiskTimeIndex(false);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> labInMemoryTimeIndexPartiallyMerged = buildInMemoryTimeIndex(true);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> labOnDiskTimeIndexPartiallyMerged = buildOnDiskTimeIndex(true);
        int i = 0;
        for (; i < timestamps.length / 2; i++) {
            chunkInMemoryTimeIndexPartiallyMerged.timeIndex.nextId(stackBuffer, timestamps[i]);
            chunkOnDiskTimeIndexPartiallyMerged.timeIndex.nextId(stackBuffer, timestamps[i]);
            labInMemoryTimeIndexPartiallyMerged.timeIndex.nextId(stackBuffer, timestamps[i]);
            labOnDiskTimeIndexPartiallyMerged.timeIndex.nextId(stackBuffer, timestamps[i]);
        }

        ((MiruDeltaTimeIndex) chunkInMemoryTimeIndexPartiallyMerged.timeIndex).merge(schema, stackBuffer);
        chunkInMemoryTimeIndexPartiallyMerged.commitable.commit();
        ((MiruDeltaTimeIndex) chunkOnDiskTimeIndexPartiallyMerged.timeIndex).merge(schema, stackBuffer);
        chunkOnDiskTimeIndexPartiallyMerged.commitable.commit();
        ((MiruDeltaTimeIndex) labInMemoryTimeIndexPartiallyMerged.timeIndex).merge(schema, stackBuffer);
        labInMemoryTimeIndexPartiallyMerged.commitable.commit();
        ((MiruDeltaTimeIndex) labOnDiskTimeIndexPartiallyMerged.timeIndex).merge(schema, stackBuffer);
        labOnDiskTimeIndexPartiallyMerged.commitable.commit();
        for (; i < timestamps.length; i++) {
            chunkInMemoryTimeIndexPartiallyMerged.timeIndex.nextId(stackBuffer, timestamps[i]);
            chunkOnDiskTimeIndexPartiallyMerged.timeIndex.nextId(stackBuffer, timestamps[i]);
            labInMemoryTimeIndexPartiallyMerged.timeIndex.nextId(stackBuffer, timestamps[i]);
            labOnDiskTimeIndexPartiallyMerged.timeIndex.nextId(stackBuffer, timestamps[i]);
        }

        return new Object[][] {
            { new MiruBitmapsRoaring(), chunkInMemoryTimeIndex },
            { new MiruBitmapsRoaring(), chunkOnDiskTimeIndex },
            { new MiruBitmapsRoaring(), chunkInMemoryTimeIndexMerged },
            { new MiruBitmapsRoaring(), chunkOnDiskTimeIndexMerged },
            { new MiruBitmapsRoaring(), chunkInMemoryTimeIndexPartiallyMerged },
            { new MiruBitmapsRoaring(), chunkOnDiskTimeIndexPartiallyMerged },
            { new MiruBitmapsRoaring(), labInMemoryTimeIndex },
            { new MiruBitmapsRoaring(), labOnDiskTimeIndex },
            { new MiruBitmapsRoaring(), labInMemoryTimeIndexMerged },
            { new MiruBitmapsRoaring(), labOnDiskTimeIndexMerged },
            { new MiruBitmapsRoaring(), labInMemoryTimeIndexPartiallyMerged },
            { new MiruBitmapsRoaring(), labOnDiskTimeIndexPartiallyMerged }
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

        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> chunkInMemoryTimeIndex = buildInMemoryTimeIndex(false);
        chunkInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> chunkOnDiskTimeIndex = buildOnDiskTimeIndex(false);
        chunkOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> labInMemoryTimeIndex = buildInMemoryTimeIndex(true);
        labInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps);
        MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> labOnDiskTimeIndex = buildOnDiskTimeIndex(true);
        labOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps);

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

        chunkInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps);
        labInMemoryTimeIndex.timeIndex.nextId(stackBuffer, timestamps);
        chunkOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps);
        labOnDiskTimeIndex.timeIndex.nextId(stackBuffer, timestamps);

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
        return IndexTestUtil.buildInMemoryContext(numberOfChunkStores, useLabIndexes, bitmaps, coord);
    }

    private MiruContext<RoaringBitmap, RoaringBitmap, RCVSSipCursor> buildOnDiskTimeIndex(boolean useLabIndexes) throws Exception {
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("logicalName"));
        return IndexTestUtil.buildOnDiskContext(numberOfChunkStores, useLabIndexes, bitmaps, coord);
    }
}
