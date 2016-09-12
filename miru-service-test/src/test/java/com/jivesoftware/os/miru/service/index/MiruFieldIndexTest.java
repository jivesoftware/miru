package com.jivesoftware.os.miru.service.index;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.bitmaps.roaring5.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.ArrayUtils;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.IndexTestUtil.buildInMemoryContext;
import static com.jivesoftware.os.miru.service.IndexTestUtil.buildOnDiskContext;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MiruFieldIndexTest {

    private final long initialChunkStoreSizeInBytes = 4_096;

    @Test(dataProvider = "miruIndexDataProvider")
    public <BM extends IBM, IBM> void testGetMissingFieldTerm(MiruBitmaps<BM, IBM> bitmaps,
        MiruFieldIndex<BM, IBM> miruFieldIndex,
        MiruBackingStorage miruBackingStorage) throws
        Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruInvertedIndex<BM, IBM> invertedIndex = miruFieldIndex.get("test", 0, new MiruTermId(FilerIO.intBytes(1)));
        assertNotNull(invertedIndex);
        assertFalse(invertedIndex.getIndex(stackBuffer).isPresent());
    }

    @Test(dataProvider = "miruIndexDataProvider")
    public <BM extends IBM, IBM> void testIndexFieldTerm(MiruBitmaps<BM, IBM> bitmaps,
        MiruFieldIndex<BM, IBM> miruFieldIndex,
        MiruBackingStorage miruBackingStorage) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        miruFieldIndex.set(0, new MiruTermId(FilerIO.intBytes(2)), new int[] { 3 }, null, stackBuffer);
        MiruInvertedIndex<BM, IBM> invertedIndex = miruFieldIndex.get("test", 0, new MiruTermId(FilerIO.intBytes(2)));
        assertNotNull(invertedIndex);
        assertTrue(invertedIndex.getIndex(stackBuffer).isPresent());
        assertTrue(bitmaps.isSet(invertedIndex.getIndex(stackBuffer).get(), 3));
    }

    @Test(dataProvider = "miruIndexDataProviderWithData")
    public <BM extends IBM, IBM> void testExpectedData(MiruBitmaps<BM, IBM> bitmaps,
        MiruFieldIndex<BM, IBM> miruFieldIndex,
        List<Integer> expected,
        MiruBackingStorage miruBackingStorage)
        throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        byte[] key = "term1".getBytes();

        MiruInvertedIndex<BM, IBM> invertedIndex = miruFieldIndex.get("test", 0, new MiruTermId(key));
        assertNotNull(invertedIndex);
        assertTrue(invertedIndex.getIndex(stackBuffer).isPresent());

        List<Integer> actual = Lists.newArrayList();
        MiruIntIterator iter = bitmaps.intIterator(invertedIndex.getIndex(stackBuffer).get());
        while (iter.hasNext()) {
            actual.add(iter.next());
        }
        assertEquals(actual, expected);
    }

    @Test(dataProvider = "miruIndexDataProviderWithData")
    public <BM extends IBM, IBM> void testStreamTermIds(MiruBitmaps<BM, IBM> bitmaps,
        MiruFieldIndex<BM, IBM> miruFieldIndex,
        List<Integer> expected,
        MiruBackingStorage miruBackingStorage)
        throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        byte[] key = "term1".getBytes();

        Set<MiruTermId> found = Sets.newHashSet();
        miruFieldIndex.streamTermIdsForField("test", 0, null, termId -> {
            found.add(termId);
            return true;
        }, stackBuffer);

        assertEquals(found.size(), 1);
        assertTrue(found.contains(new MiruTermId(key)));
    }

    @DataProvider(name = "miruIndexDataProvider")
    public Object[][] miruIndexDataProvider() throws Exception {
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("logicalName"));

        return ArrayUtils.addAll(buildIndexDataProvider(bitmaps, coord, false),
            buildIndexDataProvider(bitmaps, coord, true));
    }

    private Object[][] buildIndexDataProvider(MiruBitmapsRoaring bitmaps, MiruPartitionCoord coord, boolean useLabIndexes) throws Exception {
        MiruContext<RoaringBitmap, RoaringBitmap, ?> hybridContext = buildInMemoryContext(4, useLabIndexes, true, bitmaps, coord);
        MiruFieldIndex<RoaringBitmap, RoaringBitmap> miruInMemoryFieldIndex = hybridContext.fieldIndexProvider.getFieldIndex(
            MiruFieldType.primary);

        MiruContext<RoaringBitmap, RoaringBitmap, ?> onDiskContext = buildOnDiskContext(4, useLabIndexes, true, bitmaps, coord);
        MiruFieldIndex<RoaringBitmap, RoaringBitmap> miruOnDiskFieldIndex = onDiskContext.fieldIndexProvider.getFieldIndex(
            MiruFieldType.primary);

        return new Object[][] {
            { bitmaps, miruInMemoryFieldIndex, MiruBackingStorage.memory },
            { bitmaps, miruOnDiskFieldIndex, MiruBackingStorage.disk }
        };
    }

    @DataProvider(name = "miruIndexDataProviderWithData")
    public Object[][] miruIndexDataProviderWithData() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruTenantId tenantId = new MiruTenantId("test".getBytes());
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("logicalName"));

        return ArrayUtils.addAll(buildIndexDataProviderWithData(stackBuffer, bitmaps, coord, false),
            buildIndexDataProviderWithData(stackBuffer, bitmaps, coord, true));
    }

    private Object[][] buildIndexDataProviderWithData(StackBuffer stackBuffer,
        MiruBitmapsRoaring bitmaps,
        MiruPartitionCoord coord,
        boolean useLabIndexes) throws Exception {

        MiruContext<RoaringBitmap, RoaringBitmap, ?> hybridContext = buildInMemoryContext(4, useLabIndexes, true, bitmaps, coord);
        MiruFieldIndex<RoaringBitmap, RoaringBitmap> miruHybridFieldIndex = hybridContext.fieldIndexProvider.getFieldIndex(
            MiruFieldType.primary);
        miruHybridFieldIndex.set(0, new MiruTermId("term1".getBytes()), new int[] { 1, 2, 3 }, null, stackBuffer);

        MiruContext<RoaringBitmap, RoaringBitmap, ?> onDiskContext = buildOnDiskContext(4, useLabIndexes, true, bitmaps, coord);
        MiruFieldIndex<RoaringBitmap, RoaringBitmap> miruOnDiskFieldIndex = onDiskContext.fieldIndexProvider.getFieldIndex(
            MiruFieldType.primary);
        miruOnDiskFieldIndex.set(0, new MiruTermId("term1".getBytes()), new int[] { 1, 2, 3 }, null, stackBuffer);

        return new Object[][] {
            { bitmaps, miruHybridFieldIndex, Arrays.asList(1, 2, 3), MiruBackingStorage.memory },
            { bitmaps, miruOnDiskFieldIndex, Arrays.asList(1, 2, 3), MiruBackingStorage.disk }
        };
    }
}
