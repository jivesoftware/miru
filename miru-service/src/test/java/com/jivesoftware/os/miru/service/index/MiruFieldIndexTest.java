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
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
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
    public <BM> void testGetMissingFieldTerm(MiruBitmaps<BM> bitmaps, MiruFieldIndex<BM> miruFieldIndex, MiruBackingStorage miruBackingStorage) throws
        Exception {
        MiruInvertedIndex<BM> invertedIndex = miruFieldIndex.get(0, new MiruTermId(FilerIO.intBytes(1)));
        assertNotNull(invertedIndex);
        assertFalse(invertedIndex.getIndex().isPresent());
    }

    @Test(dataProvider = "miruIndexDataProvider")
    public <BM> void testIndexFieldTerm(MiruBitmaps<BM> bitmaps, MiruFieldIndex<BM> miruFieldIndex, MiruBackingStorage miruBackingStorage) throws Exception {
        miruFieldIndex.append(0, new MiruTermId(FilerIO.intBytes(2)), 3);
        MiruInvertedIndex<BM> invertedIndex = miruFieldIndex.get(0, new MiruTermId(FilerIO.intBytes(2)));
        assertNotNull(invertedIndex);
        assertTrue(invertedIndex.getIndex().isPresent());
        assertTrue(bitmaps.isSet(invertedIndex.getIndex().get(), 3));
    }

    @Test(dataProvider = "miruIndexDataProviderWithData")
    public <BM> void testExpectedData(MiruBitmaps<BM> bitmaps, MiruFieldIndex<BM> miruFieldIndex, List<Integer> expected, MiruBackingStorage miruBackingStorage)
        throws Exception {

        byte[] key = "term1".getBytes();

        MiruInvertedIndex<BM> invertedIndex = miruFieldIndex.get(0, new MiruTermId(key));
        assertNotNull(invertedIndex);
        assertTrue(invertedIndex.getIndex().isPresent());

        List<Integer> actual = Lists.newArrayList();
        MiruIntIterator iter = bitmaps.intIterator(invertedIndex.getIndex().get());
        while (iter.hasNext()) {
            actual.add(iter.next());
        }
        assertEquals(actual, expected);
    }

    @DataProvider(name = "miruIndexDataProvider")
    public Object[][] miruIndexDataProvider() throws Exception {
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(4);
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("localhost", 10000));

        MiruContext<EWAHCompressedBitmap, ?> hybridContext = buildInMemoryContext(4, bitmaps, coord);
        MiruFieldIndex<EWAHCompressedBitmap> miruInMemoryFieldIndex = hybridContext.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);

        MiruContext<EWAHCompressedBitmap, ?> onDiskContext = buildOnDiskContext(4, bitmaps, coord);
        MiruFieldIndex<EWAHCompressedBitmap> miruOnDiskFieldIndex = onDiskContext.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);

        return new Object[][]{
            {bitmaps, miruInMemoryFieldIndex, MiruBackingStorage.memory },
            {bitmaps, miruOnDiskFieldIndex, MiruBackingStorage.disk }
        };
    }

    @DataProvider(name = "miruIndexDataProviderWithData")
    public Object[][] miruIndexDataProviderWithData() throws Exception {
        MiruTenantId tenantId = new MiruTenantId("test".getBytes());
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(4);
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("localhost", 10000));

        MiruContext<EWAHCompressedBitmap, ?> hybridContext = buildInMemoryContext(4, bitmaps, coord);
        MiruFieldIndex<EWAHCompressedBitmap> miruHybridFieldIndex = hybridContext.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);
        miruHybridFieldIndex.append(0, new MiruTermId("term1".getBytes()), 1, 2, 3);

        MiruContext<EWAHCompressedBitmap, ?> onDiskContext = buildOnDiskContext(4, bitmaps, coord);
        MiruFieldIndex<EWAHCompressedBitmap> miruOnDiskFieldIndex = onDiskContext.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);
        miruOnDiskFieldIndex.append(0, new MiruTermId("term1".getBytes()), 1, 2, 3);

        return new Object[][]{
            {bitmaps, miruHybridFieldIndex, Arrays.asList(1, 2, 3), MiruBackingStorage.memory },
            {bitmaps, miruOnDiskFieldIndex, Arrays.asList(1, 2, 3), MiruBackingStorage.disk }
        };
    }
}
