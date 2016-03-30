package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema.Builder;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.bitmaps.roaring5.buffer.MiruBitmapsRoaringBuffer;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.IndexTestUtil;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaTimeIndex;
import java.text.DecimalFormat;
import java.util.Arrays;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MiruTimeIndexTest {

    boolean useLabIndexes = true;

    private final MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
    private final MiruTenantId tenantId = new MiruTenantId(new byte[]{1});
    private final MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("logicalName"));
    private final int numberOfChunkStores = 4;

    @Test(dataProvider = "miruTimeIndexDataProviderWithData")
    public void testClosestIdWithPresentIds(MiruTimeIndex miruTimeIndex, int capacity) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        for (int i = 0; i < capacity; i++) {
            int id = miruTimeIndex.getClosestId(i * 10, stackBuffer);
            assertEquals(id, i, "Should be equal at " + i);
        }
    }

    @Test(dataProvider = "miruTimeIndexDataProviderWithData")
    public void testClosestIdWithAbsentIds(MiruTimeIndex miruTimeIndex, int capacity) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        for (int i = 0; i < capacity; i++) {
            int id = miruTimeIndex.getClosestId(i * 10 + 1, stackBuffer);
            assertEquals(id, i + 1, "Should be equal at " + i);
        }
    }

    @Test(dataProvider = "miruTimeIndexDataProviderWithData")
    public void testExactIdWithPresentIds(MiruTimeIndex miruTimeIndex, int capacity) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        for (int i = 0; i < capacity; i++) {
            int id = miruTimeIndex.getExactId(i * 10, stackBuffer);
            assertEquals(id, i, "Should be equal at " + i);
        }
    }

    @Test(dataProvider = "miruTimeIndexDataProviderWithData")
    public void testExactIdWithAbsentIds(MiruTimeIndex miruTimeIndex, int capacity) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        for (int i = 0; i < capacity; i++) {
            int id = miruTimeIndex.getExactId(i * 10 + 1, stackBuffer);
            assertEquals(id, -1, "Should be equal at " + i);
        }
    }

    @Test(dataProvider = "miruTimeIndexDataProviderWithData")
    public void testContainsWithPresentIds(MiruTimeIndex miruTimeIndex, int capacity) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        for (int i = 0; i < capacity; i++) {
            assertTrue(miruTimeIndex.contains(Arrays.asList(i * 10L), stackBuffer)[0], "Should be true at " + i);
        }
    }

    @Test(dataProvider = "miruTimeIndexDataProviderWithData")
    public void testContainsWithAbsentIds(MiruTimeIndex miruTimeIndex, int capacity) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        for (int i = 0; i < capacity; i++) {
            assertFalse(miruTimeIndex.contains(Arrays.asList(i * 10 + 1L), stackBuffer)[0], "Should be false at " + i);
        }
    }

    @Test(dataProvider = "miruTimeIndexDataProviderWithRangeData")
    public void testLargestInclusiveTimestampIndex(MiruTimeIndex miruTimeIndex) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        // { 1, 1, 1, 3, 3, 3, 5, 5, 5 }
        assertEquals(miruTimeIndex.largestInclusiveTimestampIndex(0, stackBuffer), -1);
        assertEquals(miruTimeIndex.largestInclusiveTimestampIndex(1, stackBuffer), 2);
        assertEquals(miruTimeIndex.largestInclusiveTimestampIndex(2, stackBuffer), 2);
        assertEquals(miruTimeIndex.largestInclusiveTimestampIndex(3, stackBuffer), 5);
        assertEquals(miruTimeIndex.largestInclusiveTimestampIndex(4, stackBuffer), 5);
        assertEquals(miruTimeIndex.largestInclusiveTimestampIndex(5, stackBuffer), 8);
        assertEquals(miruTimeIndex.largestInclusiveTimestampIndex(6, stackBuffer), 8);
    }

    @Test(dataProvider = "miruTimeIndexDataProviderWithRangeData")
    public void testSmallestExclusiveTimestampIndex(MiruTimeIndex miruTimeIndex) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        // { 1, 1, 1, 3, 3, 3, 5, 5, 5 }
        assertEquals(miruTimeIndex.smallestExclusiveTimestampIndex(0, stackBuffer), 0);
        assertEquals(miruTimeIndex.smallestExclusiveTimestampIndex(1, stackBuffer), 3);
        assertEquals(miruTimeIndex.smallestExclusiveTimestampIndex(2, stackBuffer), 3);
        assertEquals(miruTimeIndex.smallestExclusiveTimestampIndex(3, stackBuffer), 6);
        assertEquals(miruTimeIndex.smallestExclusiveTimestampIndex(4, stackBuffer), 6);
        assertEquals(miruTimeIndex.smallestExclusiveTimestampIndex(5, stackBuffer), 9);
        assertEquals(miruTimeIndex.smallestExclusiveTimestampIndex(6, stackBuffer), 9);
    }

    @Test(dataProvider = "miruTimeIndexDataProviderWithoutData")
    public void testPartiallyPresentBatchIds(MiruTimeIndex miruTimeIndex) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        boolean[] contains = miruTimeIndex.contains(Arrays.asList(10L, 20L, 30L, 40L), stackBuffer);
        for (boolean contained : contains) {
            assertFalse(contained);
        }

        int[] ids = miruTimeIndex.nextId(stackBuffer, 10L, 20L, 30L, 40L);
        for (int i = 0; i < ids.length; i++) {
            assertEquals(ids[i], i);
            assertEquals(miruTimeIndex.getExactId((i + 1) * 10, stackBuffer), ids[i]);
        }

        assertEquals(miruTimeIndex.getSmallestTimestamp(), 10L);
        assertEquals(miruTimeIndex.getLargestTimestamp(), 40L);

        contains = miruTimeIndex.contains(Arrays.asList(30L, 35L, 40L, 45L), stackBuffer);
        for (int i = 0; i < contains.length; i++) {
            if (i % 2 == 1) {
                assertFalse(contains[i]);
            } else {
                assertTrue(contains[i]);
            }
        }

        ids = miruTimeIndex.nextId(stackBuffer, -1L, 35L, -1L, 45L);
        for (int i = 0; i < ids.length; i++) {
            if (i % 2 == 1) {
                assertEquals(ids[i], 4 + i / 2);
            } else {
                assertEquals(ids[i], -1);
            }
        }

        assertEquals(miruTimeIndex.getSmallestTimestamp(), 10L);
        assertEquals(miruTimeIndex.getLargestTimestamp(), 45L);
        assertEquals(miruTimeIndex.smallestExclusiveTimestampIndex(10L, stackBuffer), 1);
        assertEquals(miruTimeIndex.smallestExclusiveTimestampIndex(12L, stackBuffer), 1);
        assertEquals(miruTimeIndex.smallestExclusiveTimestampIndex(40L, stackBuffer), 5);
        assertEquals(miruTimeIndex.largestInclusiveTimestampIndex(5L, stackBuffer), -1);
        assertEquals(miruTimeIndex.largestInclusiveTimestampIndex(42L, stackBuffer), 4);
        assertEquals(miruTimeIndex.largestInclusiveTimestampIndex(45L, stackBuffer), 5);
    }

    /*
     SSD:

     int[] tryLevels = new int[]{2, 3, 4, 5};
     int[] trySegments = new int[]{4, 16, 32};
     int capacity = 1_000_000;

     InMemory capacity=1,000,000 elapsed=364

     CopyToDisk size=8,000,252 levels=2 segments=4 elapsed=18,201
     GetClosest(100) levels=2 segments=4 elapsed=18,319 avg=183

     CopyToDisk size=8,003,228 levels=2 segments=16 elapsed=18,053
     GetClosest(100) levels=2 segments=16 elapsed=1,151 avg=11

     CopyToDisk size=8,012,572 levels=2 segments=32 elapsed=18,035
     GetClosest(100) levels=2 segments=32 elapsed=308 avg=3

     CopyToDisk size=8,000,956 levels=3 segments=4 elapsed=17,877
     GetClosest(100) levels=3 segments=4 elapsed=4,592 avg=45

     CopyToDisk size=8,051,356 levels=3 segments=16 elapsed=18,108
     GetClosest(100) levels=3 segments=16 elapsed=103 avg=1

     CopyToDisk size=8,401,692 levels=3 segments=32 elapsed=18,631
     GetClosest(100) levels=3 segments=32 elapsed=40 avg=0

     CopyToDisk size=8,003,772 levels=4 segments=4 elapsed=17,856
     GetClosest(100) levels=4 segments=4 elapsed=1,152 avg=11

     CopyToDisk size=8,821,404 levels=4 segments=16 elapsed=19,242
     GetClosest(100) levels=4 segments=16 elapsed=27 avg=0

     CopyToDisk size=20,853,532 levels=4 segments=32 elapsed=38,438
     GetClosest(100) levels=4 segments=32 elapsed=42 avg=0

     CopyToDisk size=8,015,036 levels=5 segments=4 elapsed=18,023
     GetClosest(100) levels=5 segments=4 elapsed=298 avg=2

     CopyToDisk size=21,142,172 levels=5 segments=16 elapsed=38,977
     GetClosest(100) levels=5 segments=16 elapsed=29 avg=0
     */
    @Test
    public void testPerformance() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        DecimalFormat formatter = new DecimalFormat("###,###,###");
        int[] tryLevels = new int[]{3}; //{2, 3, 4, 5};
        int[] trySegments = new int[]{16}; //{4, 16, 32};
        int capacity = 100; //1_000_000;
        long start;

        for (int levels : tryLevels) {
            for (int segments : trySegments) {
                if ((long) Math.pow(segments, levels) > 1_048_576) {
                    continue; // skips 32^5
                }

                start = System.currentTimeMillis();
                MiruTimeIndex onDiskTimeIndex = IndexTestUtil.buildOnDiskContext(numberOfChunkStores, useLabIndexes, bitmaps, coord).timeIndex;
                for (int i = 0; i < capacity; i++) {
                    onDiskTimeIndex.nextId(stackBuffer, i * 10);
                }
                System.out.println("CopyToDisk"
                    + " levels=" + levels
                    + " segments=" + segments
                    + " elapsed=" + formatter.format(System.currentTimeMillis() - start));

                assertNotNull(onDiskTimeIndex);

                start = System.currentTimeMillis();
                int gets = 100;
                for (int i = 0; i < capacity; i += (capacity / gets)) {
                    int id = onDiskTimeIndex.getClosestId(i * 10, stackBuffer);
                    assertEquals(id, i);
                }
                System.out.println("GetClosest(" + gets + ")"
                    + " levels=" + levels
                    + " segments=" + segments
                    + " elapsed=" + formatter.format(System.currentTimeMillis() - start)
                    + " avg=" + formatter.format((System.currentTimeMillis() - start) / gets));
                System.out.println();
            }
        }
    }

    @DataProvider(name = "miruTimeIndexDataProviderWithoutData")
    public Object[][] miruTimeIndexDataProviderWithoutData() throws Exception {
        try {
            MiruTimeIndex miruInMemoryTimeIndex = IndexTestUtil.buildInMemoryContext(numberOfChunkStores, useLabIndexes, bitmaps, coord).timeIndex;
            MiruTimeIndex miruOnDiskTimeIndex = IndexTestUtil.buildOnDiskContext(numberOfChunkStores, useLabIndexes, bitmaps, coord).timeIndex;

            return new Object[][]{
                {miruInMemoryTimeIndex},
                {miruOnDiskTimeIndex}
            };
        } catch (Exception x) {
            System.out.println("Your data provider is hosed!");
            x.printStackTrace();
            return null;
        }
    }

    @DataProvider(name = "miruTimeIndexDataProviderWithData")
    public Object[][] miruTimeIndexDataProviderWithData() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruSchema schema = new Builder("test", 1).build();
        try {
            int capacity = 1_000;
            final long[] importValues = new long[capacity];
            for (int i = 0; i < capacity; i++) {
                importValues[i] = i * 10;
            }

            MiruTimeIndex miruInMemoryTimeIndex = IndexTestUtil.buildInMemoryContext(numberOfChunkStores, useLabIndexes, bitmaps, coord).timeIndex;
            MiruTimeIndex miruOnDiskTimeIndex = IndexTestUtil.buildOnDiskContext(numberOfChunkStores, useLabIndexes, bitmaps, coord).timeIndex;
            miruOnDiskTimeIndex.nextId(stackBuffer, importValues);
            miruInMemoryTimeIndex.nextId(stackBuffer, importValues);

            MiruTimeIndex miruInMemoryTimeIndexMerged = IndexTestUtil.buildInMemoryContext(numberOfChunkStores, useLabIndexes, bitmaps, coord).timeIndex;
            MiruTimeIndex miruOnDiskTimeIndexMerged = IndexTestUtil.buildOnDiskContext(numberOfChunkStores, useLabIndexes, bitmaps, coord).timeIndex;
            miruOnDiskTimeIndexMerged.nextId(stackBuffer, importValues);
            miruInMemoryTimeIndexMerged.nextId(stackBuffer, importValues);
            ((MiruDeltaTimeIndex) miruOnDiskTimeIndexMerged).merge(schema, stackBuffer);
            ((MiruDeltaTimeIndex) miruInMemoryTimeIndexMerged).merge(schema, stackBuffer);

            MiruTimeIndex miruInMemoryTimeIndexPartiallyMerged = IndexTestUtil.buildInMemoryContext(numberOfChunkStores, useLabIndexes, bitmaps, coord).timeIndex;
            MiruTimeIndex miruOnDiskTimeIndexPartiallyMerged = IndexTestUtil.buildOnDiskContext(numberOfChunkStores, useLabIndexes, bitmaps, coord).timeIndex;
            int i = 0;
            for (; i < importValues.length / 2; i++) {
                miruInMemoryTimeIndexPartiallyMerged.nextId(stackBuffer, importValues[i]);
                miruOnDiskTimeIndexPartiallyMerged.nextId(stackBuffer, importValues[i]);
            }
            ((MiruDeltaTimeIndex) miruInMemoryTimeIndexPartiallyMerged).merge(schema, stackBuffer);
            ((MiruDeltaTimeIndex) miruOnDiskTimeIndexPartiallyMerged).merge(schema, stackBuffer);
            for (; i < importValues.length; i++) {
                miruInMemoryTimeIndexPartiallyMerged.nextId(stackBuffer, importValues[i]);
                miruOnDiskTimeIndexPartiallyMerged.nextId(stackBuffer, importValues[i]);
            }

            return new Object[][]{
                {miruInMemoryTimeIndex, capacity},
                {miruOnDiskTimeIndex, capacity},
                {miruInMemoryTimeIndexMerged, capacity},
                {miruOnDiskTimeIndexMerged, capacity},
                {miruInMemoryTimeIndexPartiallyMerged, capacity},
                {miruOnDiskTimeIndexPartiallyMerged, capacity}
            };
        } catch (Exception x) {
            System.out.println("Your data provider is hosed!");
            x.printStackTrace();
            return null;
        }
    }

    @DataProvider(name = "miruTimeIndexDataProviderWithRangeData")
    public Object[][] miruTimeIndexDataProviderWithRangeData() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruSchema schema = new Builder("test", 1).build();
        try {
            final long[] importValues = {1, 1, 1, 3, 3, 3, 5, 5, 5};
            MiruTimeIndex miruInMemoryTimeIndex = IndexTestUtil.buildInMemoryContext(numberOfChunkStores, useLabIndexes, bitmaps, coord).timeIndex;
            MiruTimeIndex miruOnDiskTimeIndex = IndexTestUtil.buildOnDiskContext(numberOfChunkStores, useLabIndexes, bitmaps, coord).timeIndex;
            miruOnDiskTimeIndex.nextId(stackBuffer, importValues);
            miruInMemoryTimeIndex.nextId(stackBuffer, importValues);

            MiruTimeIndex miruInMemoryTimeIndexMerged = IndexTestUtil.buildInMemoryContext(numberOfChunkStores, useLabIndexes, bitmaps, coord).timeIndex;
            MiruTimeIndex miruOnDiskTimeIndexMerged = IndexTestUtil.buildOnDiskContext(numberOfChunkStores, useLabIndexes, bitmaps, coord).timeIndex;
            miruOnDiskTimeIndexMerged.nextId(stackBuffer, importValues);
            miruInMemoryTimeIndexMerged.nextId(stackBuffer, importValues);
            ((MiruDeltaTimeIndex) miruOnDiskTimeIndexMerged).merge(schema, stackBuffer);
            ((MiruDeltaTimeIndex) miruInMemoryTimeIndexMerged).merge(schema, stackBuffer);

            MiruTimeIndex miruInMemoryTimeIndexPartiallyMerged = IndexTestUtil.buildInMemoryContext(numberOfChunkStores, useLabIndexes, bitmaps, coord).timeIndex;
            MiruTimeIndex miruOnDiskTimeIndexPartiallyMerged = IndexTestUtil.buildOnDiskContext(numberOfChunkStores, useLabIndexes, bitmaps, coord).timeIndex;
            int i = 0;
            for (; i < importValues.length / 2; i++) {
                miruInMemoryTimeIndexPartiallyMerged.nextId(stackBuffer, importValues[i]);
                miruOnDiskTimeIndexPartiallyMerged.nextId(stackBuffer, importValues[i]);
            }
            ((MiruDeltaTimeIndex) miruInMemoryTimeIndexPartiallyMerged).merge(schema, stackBuffer);
            ((MiruDeltaTimeIndex) miruOnDiskTimeIndexPartiallyMerged).merge(schema, stackBuffer);
            for (; i < importValues.length; i++) {
                miruInMemoryTimeIndexPartiallyMerged.nextId(stackBuffer, importValues[i]);
                miruOnDiskTimeIndexPartiallyMerged.nextId(stackBuffer, importValues[i]);
            }

            return new Object[][]{
                {miruInMemoryTimeIndex},
                {miruOnDiskTimeIndex},
                {miruInMemoryTimeIndexMerged},
                {miruOnDiskTimeIndexMerged},
                {miruInMemoryTimeIndexPartiallyMerged},
                {miruOnDiskTimeIndexPartiallyMerged}
            };
        } catch (Exception x) {
            System.out.println("Your data provider is hosed!");
            x.printStackTrace();
            return null;
        }
    }
}
