package com.jivesoftware.os.miru.bitmaps.roaring4;

import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaTimeIndex;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.IndexTestUtil.buildInMemoryContext;
import static com.jivesoftware.os.miru.service.IndexTestUtil.buildOnDiskContext;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 *
 */
public class MiruBitmapsTimeRangeTest {

    private final int numberOfChunkStores = 4;

    @Test(dataProvider = "evenTimeIndexDataProvider")
    public <BM extends IBM, IBM> void testBuildEvenTimeRangeMask(MiruBitmaps<BM, IBM> bitmaps, MiruTimeIndex miruTimeIndex) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        final int size = (64 * 3) + 1;
        for (int lower = 0; lower <= size / 2; lower++) {
            int upper = size - 1 - lower;

            IBM bitmap = bitmaps.buildTimeRangeMask(miruTimeIndex, lower, upper, primitiveBuffer);
            if (lower == upper) {
                // the lower and upper are the same so there should be nothing left
                assertExpectedNumberOfConsecutiveBitsStartingFromN(bitmaps, bitmap, -1, 0);
            } else {
                assertExpectedNumberOfConsecutiveBitsStartingFromN(bitmaps, bitmap, lower + 1, size - 1 - 2 * lower);
            }
        }
    }

    @Test(dataProvider = "oddTimeIndexDataProvider")
    public <BM extends IBM, IBM> void testBuildOddTimeRangeMask(MiruBitmaps<BM, IBM> bitmaps, MiruTimeIndex miruTimeIndex) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        final int size = 64 * 3;
        for (int lower = 0; lower < size / 2; lower++) {
            int upper = size - 1 - lower;

            IBM bitmap = bitmaps.buildTimeRangeMask(miruTimeIndex, lower, upper, primitiveBuffer);
            if (lower == upper) {
                fail();
            } else {
                assertExpectedNumberOfConsecutiveBitsStartingFromN(bitmaps, bitmap, lower + 1, size - 1 - 2 * lower);
            }
        }
    }

    @Test(dataProvider = "singleEntryTimeIndexDataProvider")
    public <BM extends IBM, IBM> void testSingleBitTimeRange(MiruBitmaps<BM, IBM> bitmaps, MiruTimeIndex miruTimeIndex) {
        byte[] primitiveBuffer = new byte[8];
        IBM bitmap = bitmaps.buildTimeRangeMask(miruTimeIndex, 0, Long.MAX_VALUE, primitiveBuffer);

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
        byte[] primitiveBuffer = new byte[8];
        final int size = (64 * 3) + 1;
        final long[] timestamps = new long[size];
        for (int i = 0; i < size; i++) {
            timestamps[i] = i;
        }

        MiruTimeIndex miruInMemoryTimeIndex = buildInMemoryTimeIndex();
        MiruTimeIndex miruOnDiskTimeIndex = buildOnDiskTimeIndex();
        miruInMemoryTimeIndex.nextId(primitiveBuffer, timestamps);
        miruOnDiskTimeIndex.nextId(primitiveBuffer, timestamps);

        MiruTimeIndex miruInMemoryTimeIndexMerged = buildInMemoryTimeIndex();
        MiruTimeIndex miruOnDiskTimeIndexMerged = buildOnDiskTimeIndex();
        miruInMemoryTimeIndexMerged.nextId(primitiveBuffer, timestamps);
        miruOnDiskTimeIndexMerged.nextId(primitiveBuffer, timestamps);
        ((MiruDeltaTimeIndex) miruInMemoryTimeIndexMerged).merge(primitiveBuffer);
        ((MiruDeltaTimeIndex) miruOnDiskTimeIndexMerged).merge(primitiveBuffer);

        MiruTimeIndex miruInMemoryTimeIndexPartiallyMerged = buildInMemoryTimeIndex();
        MiruTimeIndex miruOnDiskTimeIndexPartiallyMerged = buildOnDiskTimeIndex();
        int i = 0;
        for (; i < timestamps.length / 2; i++) {
            miruInMemoryTimeIndexPartiallyMerged.nextId(primitiveBuffer, timestamps[i]);
            miruOnDiskTimeIndexPartiallyMerged.nextId(primitiveBuffer, timestamps[i]);

        }
        ((MiruDeltaTimeIndex) miruInMemoryTimeIndexPartiallyMerged).merge(primitiveBuffer);
        ((MiruDeltaTimeIndex) miruOnDiskTimeIndexPartiallyMerged).merge(primitiveBuffer);
        for (; i < timestamps.length; i++) {
            miruInMemoryTimeIndexPartiallyMerged.nextId(primitiveBuffer, timestamps[i]);
            miruOnDiskTimeIndexPartiallyMerged.nextId(primitiveBuffer, timestamps[i]);
        }

        return new Object[][]{
            {new MiruBitmapsRoaring(), miruInMemoryTimeIndex},
            {new MiruBitmapsRoaring(), miruOnDiskTimeIndex},
            {new MiruBitmapsRoaring(), miruInMemoryTimeIndexMerged},
            {new MiruBitmapsRoaring(), miruOnDiskTimeIndexMerged},
            {new MiruBitmapsRoaring(), miruInMemoryTimeIndexPartiallyMerged},
            {new MiruBitmapsRoaring(), miruOnDiskTimeIndexPartiallyMerged}
        };
    }

    @DataProvider(name = "oddTimeIndexDataProvider")
    public Object[][] oddTimeIndexDataProvider() throws Exception {
        byte[] primitiveBuffer = new byte[8];
        final int size = 64 * 3;
        final long[] timestamps = new long[size];
        for (int i = 0; i < size; i++) {
            timestamps[i] = i;
        }

        MiruTimeIndex miruInMemoryTimeIndex = buildInMemoryTimeIndex();
        miruInMemoryTimeIndex.nextId(primitiveBuffer, timestamps);
        MiruTimeIndex miruOnDiskTimeIndex = buildOnDiskTimeIndex();
        miruOnDiskTimeIndex.nextId(primitiveBuffer, timestamps);

        return new Object[][]{
            {new MiruBitmapsRoaring(), miruInMemoryTimeIndex},
            {new MiruBitmapsRoaring(), miruOnDiskTimeIndex}
        };
    }

    @DataProvider(name = "singleEntryTimeIndexDataProvider")
    public Object[][] singleEntryTimeIndexDataProvider() throws Exception {
        byte[] primitiveBuffer = new byte[8];
        final long[] timestamps = new long[]{System.currentTimeMillis()};
        MiruTimeIndex miruInMemoryTimeIndex = buildInMemoryTimeIndex();
        MiruTimeIndex miruOnDiskTimeIndex = buildOnDiskTimeIndex();
        miruOnDiskTimeIndex.nextId(primitiveBuffer, timestamps);
        miruInMemoryTimeIndex.nextId(primitiveBuffer, timestamps);

        return new Object[][]{
            {new MiruBitmapsRoaring(), miruInMemoryTimeIndex},
            {new MiruBitmapsRoaring(), miruOnDiskTimeIndex}
        };
    }

    private MiruTimeIndex buildInMemoryTimeIndex() throws Exception {
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("localhost", 10000));
        return buildInMemoryContext(numberOfChunkStores, bitmaps, coord).timeIndex;
    }

    private MiruTimeIndex buildOnDiskTimeIndex() throws Exception {
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("localhost", 10000));
        return buildOnDiskContext(numberOfChunkStores, bitmaps, coord).timeIndex;
    }
}
