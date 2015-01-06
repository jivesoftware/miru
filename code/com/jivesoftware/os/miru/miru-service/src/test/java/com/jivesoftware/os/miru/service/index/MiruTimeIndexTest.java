package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.index.MiruTimeIndex;
import com.jivesoftware.os.miru.service.IndexTestUtil;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.stream.allocator.MiruContextAllocator;
import java.text.DecimalFormat;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MiruTimeIndexTest {

    private final MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(100);
    private final MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
    private final MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("localhost", 10000));
    private final int numberOfChunkStores = 4;

    @Test(dataProvider = "miruTimeIndexDataProviderWithData")
    public void testClosestIdWithPresentIds(MiruTimeIndex miruTimeIndex, int capacity) throws Exception {
        for (int i = 0; i < capacity; i++) {
            int id = miruTimeIndex.getClosestId(i * 10);
            assertEquals(id, i, "Should be equal at " + i);
        }
    }

    @Test(dataProvider = "miruTimeIndexDataProviderWithData")
    public void testClosestIdWithAbsentIds(MiruTimeIndex miruTimeIndex, int capacity) throws Exception {
        for (int i = 0; i < capacity; i++) {
            int id = miruTimeIndex.getClosestId(i * 10 + 1);
            assertEquals(id, i + 1, "Should be equal at " + i);
        }
    }

    @Test(dataProvider = "miruTimeIndexDataProviderWithData")
    public void testExactIdWithPresentIds(MiruTimeIndex miruTimeIndex, int capacity) throws Exception {
        for (int i = 0; i < capacity; i++) {
            int id = miruTimeIndex.getExactId(i * 10);
            assertEquals(id, i, "Should be equal at " + i);
        }
    }

    @Test(dataProvider = "miruTimeIndexDataProviderWithData")
    public void testExactIdWithAbsentIds(MiruTimeIndex miruTimeIndex, int capacity) throws Exception {
        for (int i = 0; i < capacity; i++) {
            int id = miruTimeIndex.getExactId(i * 10 + 1);
            assertEquals(id, -1, "Should be equal at " + i);
        }
    }

    @Test(dataProvider = "miruTimeIndexDataProviderWithData")
    public void testContainsWithPresentIds(MiruTimeIndex miruTimeIndex, int capacity) throws Exception {
        for (int i = 0; i < capacity; i++) {
            assertTrue(miruTimeIndex.contains(i * 10), "Should be true at " + i);
        }
    }

    @Test(dataProvider = "miruTimeIndexDataProviderWithData")
    public void testContainsWithAbsentIds(MiruTimeIndex miruTimeIndex, int capacity) throws Exception {
        for (int i = 0; i < capacity; i++) {
            assertFalse(miruTimeIndex.contains(i * 10 + 1), "Should be false at " + i);
        }
    }

    @Test(dataProvider = "miruTimeIndexDataProviderWithRangeData")
    public void testLargestInclusiveTimestampIndex(MiruTimeIndex miruTimeIndex) throws Exception {
        // { 1, 1, 1, 3, 3, 3, 5, 5, 5 }
        assertEquals(miruTimeIndex.largestInclusiveTimestampIndex(0), -1);
        assertEquals(miruTimeIndex.largestInclusiveTimestampIndex(1), 2);
        assertEquals(miruTimeIndex.largestInclusiveTimestampIndex(2), 2);
        assertEquals(miruTimeIndex.largestInclusiveTimestampIndex(3), 5);
        assertEquals(miruTimeIndex.largestInclusiveTimestampIndex(4), 5);
        assertEquals(miruTimeIndex.largestInclusiveTimestampIndex(5), 8);
        assertEquals(miruTimeIndex.largestInclusiveTimestampIndex(6), 8);
    }

    @Test(dataProvider = "miruTimeIndexDataProviderWithRangeData")
    public void testSmallestExclusiveTimestampIndex(MiruTimeIndex miruTimeIndex) throws Exception {
        // { 1, 1, 1, 3, 3, 3, 5, 5, 5 }
        assertEquals(miruTimeIndex.smallestExclusiveTimestampIndex(0), 0);
        assertEquals(miruTimeIndex.smallestExclusiveTimestampIndex(1), 3);
        assertEquals(miruTimeIndex.smallestExclusiveTimestampIndex(2), 3);
        assertEquals(miruTimeIndex.smallestExclusiveTimestampIndex(3), 6);
        assertEquals(miruTimeIndex.smallestExclusiveTimestampIndex(4), 6);
        assertEquals(miruTimeIndex.smallestExclusiveTimestampIndex(5), 9);
        assertEquals(miruTimeIndex.smallestExclusiveTimestampIndex(6), 9);
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
        DecimalFormat formatter = new DecimalFormat("###,###,###");
        int[] tryLevels = new int[] { 3 }; //{2, 3, 4, 5};
        int[] trySegments = new int[] { 16 }; //{4, 16, 32};
        int capacity = 100; //1_000_000;
        long start = System.currentTimeMillis();

        MiruTimeIndex inMemoryTimeIndex = IndexTestUtil.buildHybridContextAllocator(numberOfChunkStores, 10, true, 64).allocate(bitmaps, coord).timeIndex;
        for (int i = 0; i < capacity; i++) {
            inMemoryTimeIndex.nextId(i * 10);
        }
        System.out.println("InMemory" +
            " capacity=" + formatter.format(capacity) +
            " elapsed=" + formatter.format(System.currentTimeMillis() - start));
        System.out.println();

        MiruContextAllocator onDiskContextAllocator = IndexTestUtil.buildOnDiskContextAllocator(numberOfChunkStores, 10, 64);

        for (int levels : tryLevels) {
            for (int segments : trySegments) {
                if ((long) Math.pow(segments, levels) > 1_048_576) {
                    continue; // skips 32^5
                }

                start = System.currentTimeMillis();
                MiruTimeIndex onDiskTimeIndex = onDiskContextAllocator.allocate(bitmaps, coord).timeIndex;
                ((BulkImport) onDiskTimeIndex).bulkImport(tenantId, (BulkExport) inMemoryTimeIndex);
                System.out.println("CopyToDisk" +
                    " size=" + formatter.format(onDiskTimeIndex.sizeOnDisk()) +
                    " levels=" + levels +
                    " segments=" + segments +
                    " elapsed=" + formatter.format(System.currentTimeMillis() - start));

                assertNotNull(onDiskTimeIndex);

                start = System.currentTimeMillis();
                int gets = 100;
                for (int i = 0; i < capacity; i += (capacity / gets)) {
                    int id = onDiskTimeIndex.getClosestId(i * 10);
                    assertEquals(id, i);
                }
                System.out.println("GetClosest(" + gets + ")" +
                    " levels=" + levels +
                    " segments=" + segments +
                    " elapsed=" + formatter.format(System.currentTimeMillis() - start) +
                    " avg=" + formatter.format((System.currentTimeMillis() - start) / gets));
                System.out.println();
            }
        }
    }

    @DataProvider(name = "miruTimeIndexDataProviderWithData")
    public Object[][] miruTimeIndexDataProviderWithData() throws Exception {
        int capacity = 1_000;

        // Set up and import in-memory implementation
        MiruTimeIndex miruInMemoryTimeIndex = IndexTestUtil.buildHybridContextAllocator(numberOfChunkStores, 10, true, 64).allocate(bitmaps, coord).timeIndex;

        final long[] importValues = new long[capacity];
        for (int i = 0; i < capacity; i++) {
            importValues[i] = i * 10;
        }
        for (long timestamp : importValues) {
            miruInMemoryTimeIndex.nextId(timestamp);
        }

        // Set up and import on-disk implementation
        MiruTimeIndex miruOnDiskTimeIndex = IndexTestUtil.buildOnDiskContextAllocator(numberOfChunkStores, 10, 64).allocate(bitmaps, coord).timeIndex;
        ((BulkImport) miruOnDiskTimeIndex).bulkImport(tenantId, (BulkExport) miruInMemoryTimeIndex);

        return new Object[][] {
            //{ miruInMemoryTimeIndex, capacity },
            { miruOnDiskTimeIndex, capacity }
        };
    }

    @DataProvider(name = "miruTimeIndexDataProviderWithRangeData")
    public Object[][] miruTimeIndexDataProviderWithRangeData() throws Exception {
        // Set up and import in-memory implementation
        MiruTimeIndex miruInMemoryTimeIndex = IndexTestUtil.buildHybridContextAllocator(numberOfChunkStores, 10, true, 64).allocate(bitmaps, coord).timeIndex;

        final long[] importValues = { 1, 1, 1, 3, 3, 3, 5, 5, 5 };

        for (long timestamp : importValues) {
            miruInMemoryTimeIndex.nextId(timestamp);
        }

        // Set up and import on-disk implementation
        MiruTimeIndex miruOnDiskTimeIndex = IndexTestUtil.buildOnDiskContextAllocator(numberOfChunkStores, 10, 64).allocate(bitmaps, coord).timeIndex;
        ((BulkImport) miruOnDiskTimeIndex).bulkImport(tenantId, (BulkExport) miruInMemoryTimeIndex);

        return new Object[][] {
            //{ miruInMemoryTimeIndex },
            { miruOnDiskTimeIndex }
        };
    }
}
