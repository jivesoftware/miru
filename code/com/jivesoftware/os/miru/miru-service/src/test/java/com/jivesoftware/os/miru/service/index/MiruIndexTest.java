package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryInvertedIndex;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MiruIndexTest {

    private long initialChunkStoreSizeInBytes = 4_096;

    @Test(dataProvider = "miruIndexDataProvider")
    public <BM> void testGetMissingFieldTerm(MiruBitmaps<BM> bitmaps, MiruIndex<BM> miruIndex, MiruBackingStorage miruBackingStorage) throws Exception {
        Optional<MiruInvertedIndex<BM>> invertedIndex = miruIndex.get(1, 1);
        assertNotNull(invertedIndex);
        assertFalse(invertedIndex.isPresent());
    }

    @Test(dataProvider = "miruIndexDataProvider")
    public <BM> void testEmptyIndexSize(MiruBitmaps<BM> bitmaps, MiruIndex<BM> miruIndex, MiruBackingStorage miruBackingStorage) throws Exception {
        long sizeInBytes = miruIndex.sizeInMemory() + miruIndex.sizeOnDisk();
        if (miruBackingStorage.equals(MiruBackingStorage.memory)) {
            assertEquals(sizeInBytes, 0);
        } else if (miruBackingStorage.equals(MiruBackingStorage.disk)) {
            // Nothing added to MapStore, so nothing is allocated on disk
            long initialMapStoreSizeInBytes = 0;

            assertEquals(sizeInBytes, initialMapStoreSizeInBytes); // chunk store is shared and not included in index size
        }
    }

    @Test(dataProvider = "miruIndexDataProvider")
    public <BM> void testIndexFieldTerm(MiruBitmaps<BM> bitmaps, MiruIndex<BM> miruIndex, MiruBackingStorage miruBackingStorage) throws Exception {
        miruIndex.index(1, 2, 3);
        Optional<MiruInvertedIndex<BM>> invertedIndex = miruIndex.get(1, 2);
        assertNotNull(invertedIndex);
        assertTrue(invertedIndex.isPresent());
        assertTrue(bitmaps.isSet(invertedIndex.get().getIndex(), 3));
    }

    @Test(dataProvider = "miruIndexDataProviderWithData")
    public <BM> void testExpectedData(MiruIndex<BM> miruIndex, Map<Long, MiruInvertedIndex> importData, MiruBackingStorage miruBackingStorage) throws
            Exception {
        IndexKeyFunction indexKeyFunction = new IndexKeyFunction();
        long key = indexKeyFunction.getKey(1, 2);
        MiruInvertedIndex expected = importData.get(key);

        long sizeInBytes = miruIndex.sizeInMemory() + miruIndex.sizeOnDisk();
        if (miruBackingStorage.equals(MiruBackingStorage.memory)) {
            long expectedSizeInBytes = importData.get(key).sizeInMemory() + importData.get(key).sizeOnDisk();
            assertEquals(sizeInBytes, expectedSizeInBytes);
        } else if (miruBackingStorage.equals(MiruBackingStorage.disk)) {
            // See MapStore.cost() for more information. FileBackedMemMappedByteBufferFactory.allocate() adds the extra byte
            long initialMapStoreSizeInBytes = 2_426 + 1;

            assertEquals(sizeInBytes, initialMapStoreSizeInBytes); // chunk store is shared and not included in index size
        }

        Optional<MiruInvertedIndex<BM>> invertedIndex = miruIndex.get(1, 2);
        assertNotNull(invertedIndex);
        assertTrue(invertedIndex.isPresent());

        assertNotNull(expected);
        assertEquals(invertedIndex.get().getIndex(), expected.getIndex());
    }

    @DataProvider(name = "miruIndexDataProvider")
    public Object[][] miruIndexDataProvider() throws Exception {
        MiruInMemoryIndex<EWAHCompressedBitmap> miruInMemoryIndex = new MiruInMemoryIndex<>(new MiruBitmapsEWAH(4), new HeapByteBufferFactory());

        String[] mapDirs = new String[] {
            Files.createTempDirectory("map").toFile().getAbsolutePath(),
            Files.createTempDirectory("map").toFile().getAbsolutePath()
        };
        String[] swapDirs = new String[] {
            Files.createTempDirectory("swap").toFile().getAbsolutePath(),
            Files.createTempDirectory("swap").toFile().getAbsolutePath()
        };
        String chunksDir = Files.createTempDirectory("chunk").toFile().getAbsolutePath();
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunksDir, "data", initialChunkStoreSizeInBytes, false);
        MultiChunkStore multiChunkStore = new MultiChunkStore(chunkStore);
        MiruOnDiskIndex<EWAHCompressedBitmap> miruOnDiskIndex = new MiruOnDiskIndex<>(new MiruBitmapsEWAH(4), mapDirs, swapDirs, multiChunkStore);

        return new Object[][] {
                { new MiruBitmapsEWAH(4), miruInMemoryIndex, MiruBackingStorage.memory },
                { new MiruBitmapsEWAH(4), miruOnDiskIndex, MiruBackingStorage.disk }
        };
    }

    @DataProvider(name = "miruIndexDataProviderWithData")
    public Object[][] miruIndexDataProviderWithData() throws Exception {
        IndexKeyFunction indexKeyFunction = new IndexKeyFunction();
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruInMemoryIndex<EWAHCompressedBitmap> miruInMemoryIndex = new MiruInMemoryIndex<>(new MiruBitmapsEWAH(4), new HeapByteBufferFactory());

        EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
        bitmap.set(1);
        bitmap.set(2);
        bitmap.set(3);
        MiruInvertedIndex<EWAHCompressedBitmap> invertedIndex = new MiruInMemoryInvertedIndex<>(new MiruBitmapsEWAH(4));
        invertedIndex.or(bitmap);

        long key = indexKeyFunction.getKey(1, 2);
        final Map<Long, MiruInvertedIndex<EWAHCompressedBitmap>> importData = ImmutableMap.of(
                key, invertedIndex
        );
        miruInMemoryIndex.bulkImport(tenantId, new BulkExport<Iterator<BulkEntry<Long, MiruInvertedIndex<EWAHCompressedBitmap>>>>() {
            @Override
            public Iterator<BulkEntry<Long, MiruInvertedIndex<EWAHCompressedBitmap>>> bulkExport(MiruTenantId tenantId) throws Exception {
                return Iterators.transform(importData.entrySet().iterator(), BulkEntry.<Long, MiruInvertedIndex<EWAHCompressedBitmap>>fromMapEntry());
            }
        });

        String[] mapDirs = new String[] {
            Files.createTempDirectory("map").toFile().getAbsolutePath(),
            Files.createTempDirectory("map").toFile().getAbsolutePath()
        };
        String[] swapDirs = new String[] {
            Files.createTempDirectory("swap").toFile().getAbsolutePath(),
            Files.createTempDirectory("swap").toFile().getAbsolutePath()
        };
        String chunksDir = Files.createTempDirectory("chunk").toFile().getAbsolutePath();
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunksDir, "data", initialChunkStoreSizeInBytes, false);
        MultiChunkStore multiChunkStore = new MultiChunkStore(chunkStore);
        MiruOnDiskIndex<EWAHCompressedBitmap> miruOnDiskIndex = new MiruOnDiskIndex<>(new MiruBitmapsEWAH(4), mapDirs, swapDirs, multiChunkStore);
        miruOnDiskIndex.bulkImport(tenantId, miruInMemoryIndex);

        return new Object[][] {
                { miruInMemoryIndex, importData, MiruBackingStorage.memory },
                { miruOnDiskIndex, importData, MiruBackingStorage.disk }
        };
    }
}
