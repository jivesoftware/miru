package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStore;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.jive.utils.io.FilerIO;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryInvertedIndex;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MiruIndexTest {

    private long initialChunkStoreSizeInBytes = 4096;

    @Test(dataProvider = "miruIndexDataProvider")
    public <BM> void testGetMissingFieldTerm(MiruBitmaps<BM> bitmaps, MiruIndex miruIndex, MiruBackingStorage miruBackingStorage) throws Exception {
        Optional<MiruInvertedIndex> invertedIndex = miruIndex.get(1, 1);
        assertNotNull(invertedIndex);
        assertFalse(invertedIndex.isPresent());
    }

    @Test(dataProvider = "miruIndexDataProvider")
    public <BM> void testEmptyIndexSize(MiruBitmaps<BM> bitmaps, MiruIndex miruIndex, MiruBackingStorage miruBackingStorage) throws Exception {
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
    public <BM> void testIndexFieldTerm(MiruBitmaps<BM> bitmaps, MiruIndex miruIndex, MiruBackingStorage miruBackingStorage) throws Exception {
        miruIndex.index(1, 2, 3);
        Optional<MiruInvertedIndex<BM>> invertedIndex = miruIndex.get(1, 2);
        assertNotNull(invertedIndex);
        assertTrue(invertedIndex.isPresent());
        assertTrue(bitmaps.isSet(invertedIndex.get().getIndex(),3));
    }

    @Test(dataProvider = "miruIndexDataProviderWithData")
    public void testExpectedData(MiruIndex miruIndex, Map<Long, MiruInvertedIndex> importData, MiruBackingStorage miruBackingStorage) throws Exception {
        long fieldTermId = FilerIO.bytesLong(FilerIO.intArrayToByteArray(new int[] { 1, 2 }));
        MiruInvertedIndex expected = importData.get(fieldTermId);

        long sizeInBytes = miruIndex.sizeInMemory() + miruIndex.sizeOnDisk();
        if (miruBackingStorage.equals(MiruBackingStorage.memory)) {
            long expectedSizeInBytes = importData.get(fieldTermId).sizeInMemory() + importData.get(fieldTermId).sizeOnDisk();
            assertEquals(sizeInBytes, expectedSizeInBytes);
        } else if (miruBackingStorage.equals(MiruBackingStorage.disk)) {
            // See MapStore.cost() for more information. FileBackedMemMappedByteBufferFactory.allocate() adds the extra byte
            long initialMapStoreSizeInBytes = 2426 + 1;

            assertEquals(sizeInBytes, initialMapStoreSizeInBytes); // chunk store is shared and not included in index size
        }

        Optional<MiruInvertedIndex> invertedIndex = miruIndex.get(1, 2);
        assertNotNull(invertedIndex);
        assertTrue(invertedIndex.isPresent());

        assertNotNull(expected);
        assertEquals(invertedIndex.get().getIndex(), expected.getIndex());
    }

    @DataProvider(name = "miruIndexDataProvider")
    public Object[][] miruIndexDataProvider() throws Exception {
        MiruInMemoryIndex miruInMemoryIndex = new MiruInMemoryIndex(new MiruBitmapsEWAH(4));

        File mapDir = Files.createTempDirectory("map").toFile();
        File swapDir = Files.createTempDirectory("swap").toFile();
        Path chunksDir = Files.createTempDirectory("chunks");
        File chunks = new File(chunksDir.toFile(), "chunks.data");
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunks.getAbsolutePath(), initialChunkStoreSizeInBytes, false);
        MiruOnDiskIndex miruOnDiskIndex = new MiruOnDiskIndex(new MiruBitmapsEWAH(4), mapDir, swapDir, chunkStore);

        return new Object[][] {
            { new MiruBitmapsEWAH(4), miruInMemoryIndex, MiruBackingStorage.memory },
            { new MiruBitmapsEWAH(4), miruOnDiskIndex, MiruBackingStorage.disk }
        };
    }

    @DataProvider(name = "miruIndexDataProviderWithData")
    public Object[][] miruIndexDataProviderWithData() throws Exception {
        MiruInMemoryIndex miruInMemoryIndex = new MiruInMemoryIndex(new MiruBitmapsEWAH(4));

        EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
        bitmap.set(1);
        bitmap.set(2);
        bitmap.set(3);
        MiruInvertedIndex invertedIndex = new MiruInMemoryInvertedIndex(new MiruBitmapsEWAH(4));
        invertedIndex.or(bitmap);

        long key = FilerIO.bytesLong(FilerIO.intArrayToByteArray(new int[] { 1, 2 }));
        final Map<Long, MiruInvertedIndex> importData = ImmutableMap.of(
            key, invertedIndex
        );
        miruInMemoryIndex.bulkImport(new BulkExport<Map<Long, MiruInvertedIndex>>() {
            @Override
            public Map<Long, MiruInvertedIndex> bulkExport() throws Exception {
                return importData;
            }
        });

        File mapDir = Files.createTempDirectory("map").toFile();
        File swapDir = Files.createTempDirectory("swap").toFile();
        Path chunksDir = Files.createTempDirectory("chunks");
        File chunks = new File(chunksDir.toFile(), "chunks.data");
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunks.getAbsolutePath(), initialChunkStoreSizeInBytes, false);
        MiruOnDiskIndex miruOnDiskIndex = new MiruOnDiskIndex(new MiruBitmapsEWAH(4), mapDir, swapDir, chunkStore);
        miruOnDiskIndex.bulkImport(miruInMemoryIndex);

        return new Object[][] {
            { miruInMemoryIndex, importData, MiruBackingStorage.memory },
            { miruOnDiskIndex, importData, MiruBackingStorage.disk }
        };
    }
}
