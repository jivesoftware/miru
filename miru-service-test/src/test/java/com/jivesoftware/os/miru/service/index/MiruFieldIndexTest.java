package com.jivesoftware.os.miru.service.index;

import com.google.common.collect.Lists;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.bitmaps.ewah.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import java.util.Arrays;
import java.util.List;
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
    public <BM extends IBM, IBM> void testGetMissingFieldTerm(MiruBitmaps<BM, IBM> bitmaps, MiruFieldIndex<BM> miruFieldIndex, MiruBackingStorage miruBackingStorage) throws
        Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruInvertedIndex<BM> invertedIndex = miruFieldIndex.get(0, new MiruTermId(FilerIO.intBytes(1)));
        assertNotNull(invertedIndex);
        assertFalse(invertedIndex.getIndex(primitiveBuffer).isPresent());
    }

    @Test(dataProvider = "miruIndexDataProvider")
    public <BM extends IBM, IBM> void testIndexFieldTerm(MiruBitmaps<BM, IBM> bitmaps, MiruFieldIndex<BM> miruFieldIndex, MiruBackingStorage miruBackingStorage) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        miruFieldIndex.append(0, new MiruTermId(FilerIO.intBytes(2)), new int[]{3}, null, primitiveBuffer);
        MiruInvertedIndex<BM> invertedIndex = miruFieldIndex.get(0, new MiruTermId(FilerIO.intBytes(2)));
        assertNotNull(invertedIndex);
        assertTrue(invertedIndex.getIndex(primitiveBuffer).isPresent());
        assertTrue(bitmaps.isSet(invertedIndex.getIndex(primitiveBuffer).get(), 3));
    }

    @Test(dataProvider = "miruIndexDataProviderWithData")
    public <BM extends IBM, IBM> void testExpectedData(MiruBitmaps<BM, IBM> bitmaps, MiruFieldIndex<BM> miruFieldIndex, List<Integer> expected, MiruBackingStorage miruBackingStorage)
        throws Exception {
        byte[] primitiveBuffer = new byte[8];
        byte[] key = "term1".getBytes();

        MiruInvertedIndex<BM> invertedIndex = miruFieldIndex.get(0, new MiruTermId(key));
        assertNotNull(invertedIndex);
        assertTrue(invertedIndex.getIndex(primitiveBuffer).isPresent());

        List<Integer> actual = Lists.newArrayList();
        MiruIntIterator iter = bitmaps.intIterator(invertedIndex.getIndex(primitiveBuffer).get());
        while (iter.hasNext()) {
            actual.add(iter.next());
        }
        assertEquals(actual, expected);
    }

    @DataProvider(name = "miruIndexDataProvider")
    public Object[][] miruIndexDataProvider() throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(4);
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("localhost", 10000));

        MiruContext<EWAHCompressedBitmap, ?> hybridContext = buildInMemoryContext(4, bitmaps, coord);
        MiruFieldIndex<EWAHCompressedBitmap> miruInMemoryFieldIndex = hybridContext.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);

        MiruContext<EWAHCompressedBitmap, ?> onDiskContext = buildOnDiskContext(4, bitmaps, coord);
        MiruFieldIndex<EWAHCompressedBitmap> miruOnDiskFieldIndex = onDiskContext.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);

        return new Object[][]{
            {bitmaps, miruInMemoryFieldIndex, MiruBackingStorage.memory},
            {bitmaps, miruOnDiskFieldIndex, MiruBackingStorage.disk}
        };
    }

    @DataProvider(name = "miruIndexDataProviderWithData")
    public Object[][] miruIndexDataProviderWithData() throws Exception {
        byte[] primitiveBuffer = new byte[8];
        MiruTenantId tenantId = new MiruTenantId("test".getBytes());
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(4);
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("localhost", 10000));

        MiruContext<EWAHCompressedBitmap, ?> hybridContext = buildInMemoryContext(4, bitmaps, coord);
        MiruFieldIndex<EWAHCompressedBitmap> miruHybridFieldIndex = hybridContext.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);
        miruHybridFieldIndex.append(0, new MiruTermId("term1".getBytes()), new int[]{1, 2, 3}, null, primitiveBuffer);

        MiruContext<EWAHCompressedBitmap, ?> onDiskContext = buildOnDiskContext(4, bitmaps, coord);
        MiruFieldIndex<EWAHCompressedBitmap> miruOnDiskFieldIndex = onDiskContext.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);
        miruOnDiskFieldIndex.append(0, new MiruTermId("term1".getBytes()), new int[]{1, 2, 3}, null, primitiveBuffer);

        return new Object[][]{
            {bitmaps, miruHybridFieldIndex, Arrays.asList(1, 2, 3), MiruBackingStorage.memory},
            {bitmaps, miruOnDiskFieldIndex, Arrays.asList(1, 2, 3), MiruBackingStorage.disk}
        };
    }
}
