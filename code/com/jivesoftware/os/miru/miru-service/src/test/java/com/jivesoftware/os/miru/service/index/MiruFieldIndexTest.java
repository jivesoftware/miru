package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.filer.chunk.store.ChunkStore;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.ByteBufferProvider;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.keyed.store.PartitionedMapChunkBackedKeyedStore;
import com.jivesoftware.os.filer.keyed.store.VariableKeySizeMapChunkBackedKeyedStore;
import com.jivesoftware.os.filer.map.store.ByteBufferProviderBackedMapChunkFactory;
import com.jivesoftware.os.filer.map.store.BytesObjectMapStore;
import com.jivesoftware.os.filer.map.store.FileBackedMapChunkFactory;
import com.jivesoftware.os.filer.map.store.PassThroughKeyMarshaller;
import com.jivesoftware.os.filer.map.store.VariableKeySizeBytesObjectMapStore;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskFieldIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruHybridFieldIndex;
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

public class MiruFieldIndexTest {

    private long initialChunkStoreSizeInBytes = 4_096;

    @Test(dataProvider = "miruIndexDataProvider")
    public <BM> void testGetMissingFieldTerm(MiruBitmaps<BM> bitmaps, MiruFieldIndex<BM> miruFieldIndex, MiruBackingStorage miruBackingStorage) throws
        Exception {
        Optional<MiruInvertedIndex<BM>> invertedIndex = miruFieldIndex.get(0, new MiruTermId(FilerIO.intBytes(1)));
        assertNotNull(invertedIndex);
        assertFalse(invertedIndex.isPresent());
    }

    @Test(dataProvider = "miruIndexDataProvider")
    public <BM> void testEmptyIndexSize(MiruBitmaps<BM> bitmaps, MiruFieldIndex<BM> miruFieldIndex, MiruBackingStorage miruBackingStorage) throws Exception {
        long sizeInBytes = miruFieldIndex.sizeInMemory() + miruFieldIndex.sizeOnDisk();
        if (miruBackingStorage.equals(MiruBackingStorage.memory)) {
            assertEquals(sizeInBytes, 0);
        } else if (miruBackingStorage.equals(MiruBackingStorage.disk)) {
            // Nothing added to MapStore, so nothing is allocated on disk
            long initialMapStoreSizeInBytes = 0;

            assertEquals(sizeInBytes, initialMapStoreSizeInBytes); // chunk store is shared and not included in index size
        }
    }

    @Test(dataProvider = "miruIndexDataProvider")
    public <BM> void testIndexFieldTerm(MiruBitmaps<BM> bitmaps, MiruFieldIndex<BM> miruFieldIndex, MiruBackingStorage miruBackingStorage) throws Exception {
        miruFieldIndex.index(0, new MiruTermId(FilerIO.intBytes(2)), 3);
        Optional<MiruInvertedIndex<BM>> invertedIndex = miruFieldIndex.get(0, new MiruTermId(FilerIO.intBytes(2)));
        assertNotNull(invertedIndex);
        assertTrue(invertedIndex.isPresent());
        assertTrue(bitmaps.isSet(invertedIndex.get().getIndex(), 3));
    }

    @Test(dataProvider = "miruIndexDataProviderWithData")
    public <BM> void testExpectedData(MiruFieldIndex<BM> miruFieldIndex, Map<MiruIBA, MiruInvertedIndex> importData, MiruBackingStorage miruBackingStorage)
        throws Exception {

        byte[] key = FilerIO.intBytes(2);
        MiruInvertedIndex expected = importData.get(new MiruIBA(key));

        Optional<MiruInvertedIndex<BM>> invertedIndex = miruFieldIndex.get(0, new MiruTermId(FilerIO.intBytes(2)));
        assertNotNull(invertedIndex);
        assertTrue(invertedIndex.isPresent());

        assertNotNull(expected);
        assertEquals(invertedIndex.get().getIndex(), expected.getIndex());
    }

    @DataProvider(name = "miruIndexDataProvider")
    public Object[][] miruIndexDataProvider() throws Exception {

        @SuppressWarnings("unchecked")
        VariableKeySizeBytesObjectMapStore<byte[], MiruInvertedIndex<EWAHCompressedBitmap>>[] indexes = new VariableKeySizeBytesObjectMapStore[] {
            new VariableKeySizeBytesObjectMapStore<byte[], MiruInvertedIndex<EWAHCompressedBitmap>>(
                new BytesObjectMapStore[] {
                    new BytesObjectMapStore("16", 16, null,
                        new ByteBufferProviderBackedMapChunkFactory(16, true, 0, false, 10, new ByteBufferProvider("field-0", new HeapByteBufferFactory())),
                        PassThroughKeyMarshaller.INSTANCE)
                },
                PassThroughKeyMarshaller.INSTANCE)
        };
        MiruHybridFieldIndex<EWAHCompressedBitmap> miruInMemoryFieldIndex = new MiruHybridFieldIndex<>(new MiruBitmapsEWAH(4), indexes);

        String[] mapDirs = new String[] {
            Files.createTempDirectory("map").toFile().getAbsolutePath(),
            Files.createTempDirectory("map").toFile().getAbsolutePath()
        };
        String[] swapDirs = new String[] {
            Files.createTempDirectory("swap").toFile().getAbsolutePath(),
            Files.createTempDirectory("swap").toFile().getAbsolutePath()
        };
        String chunksDir = Files.createTempDirectory("chunk").toFile().getAbsolutePath();
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunksDir, "data", initialChunkStoreSizeInBytes, false, 8);
        MultiChunkStore multiChunkStore = new MultiChunkStore(chunkStore);

        VariableKeySizeMapChunkBackedKeyedStore[] onDiskIndexes = new VariableKeySizeMapChunkBackedKeyedStore[1];
        VariableKeySizeMapChunkBackedKeyedStore.Builder builder = new VariableKeySizeMapChunkBackedKeyedStore.Builder();
        builder.add(16, new PartitionedMapChunkBackedKeyedStore(
            new FileBackedMapChunkFactory(16, true, 8, false, 100, mapDirs),
            new FileBackedMapChunkFactory(16, true, 8, false, 100, swapDirs),
            multiChunkStore,
            4)); //TODO expose number of partitions
        onDiskIndexes[0] = builder.build();
        MiruOnDiskFieldIndex<EWAHCompressedBitmap> miruOnDiskFieldIndex = new MiruOnDiskFieldIndex<>(new MiruBitmapsEWAH(4), onDiskIndexes);

        return new Object[][] {
            { new MiruBitmapsEWAH(4), miruInMemoryFieldIndex, MiruBackingStorage.memory },
            { new MiruBitmapsEWAH(4), miruOnDiskFieldIndex, MiruBackingStorage.disk }
        };
    }

    @DataProvider(name = "miruIndexDataProviderWithData")
    public Object[][] miruIndexDataProviderWithData() throws Exception {
        MiruTenantId tenantId = new MiruTenantId(FilerIO.intBytes(1));

        @SuppressWarnings("unchecked")
        VariableKeySizeBytesObjectMapStore<byte[], MiruInvertedIndex<EWAHCompressedBitmap>>[] indexes = new VariableKeySizeBytesObjectMapStore[] {
            new VariableKeySizeBytesObjectMapStore<byte[], MiruInvertedIndex<EWAHCompressedBitmap>>(
                new BytesObjectMapStore[] {
                    new BytesObjectMapStore("16", 16, null,
                        new ByteBufferProviderBackedMapChunkFactory(16, true, 0, false, 10, new ByteBufferProvider("field-0", new HeapByteBufferFactory())),
                        PassThroughKeyMarshaller.INSTANCE)
                },
                PassThroughKeyMarshaller.INSTANCE)
        };
        MiruHybridFieldIndex<EWAHCompressedBitmap> miruInMemoryFieldIndex = new MiruHybridFieldIndex<>(new MiruBitmapsEWAH(4), indexes);

        EWAHCompressedBitmap bitmap = new EWAHCompressedBitmap();
        bitmap.set(1);
        bitmap.set(2);
        bitmap.set(3);
        MiruInvertedIndex<EWAHCompressedBitmap> invertedIndex = new MiruInMemoryInvertedIndex<>(new MiruBitmapsEWAH(4));
        invertedIndex.or(bitmap);

        byte[] key = FilerIO.intBytes(2);
        final Map<MiruIBA, MiruInvertedIndex<EWAHCompressedBitmap>> importData = ImmutableMap.of(new MiruIBA(key), invertedIndex);
        miruInMemoryFieldIndex.bulkImport(tenantId, new BulkExport<Iterator<Iterator<BulkEntry<byte[], MiruInvertedIndex<EWAHCompressedBitmap>>>>>() {
            @Override
            public Iterator<Iterator<BulkEntry<byte[], MiruInvertedIndex<EWAHCompressedBitmap>>>> bulkExport(MiruTenantId tenantId) throws Exception {
                return Iterators.singletonIterator(Iterators.transform(importData.entrySet().iterator(),
                    new Function<Map.Entry<MiruIBA, MiruInvertedIndex<EWAHCompressedBitmap>>, BulkEntry<byte[], MiruInvertedIndex<EWAHCompressedBitmap>>>() {
                        @Override
                        public BulkEntry<byte[], MiruInvertedIndex<EWAHCompressedBitmap>> apply(
                            Map.Entry<MiruIBA, MiruInvertedIndex<EWAHCompressedBitmap>> input) {
                            return new BulkEntry<>(input.getKey().getBytes(), input.getValue());
                        }
                    }));
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
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunksDir, "data", initialChunkStoreSizeInBytes, false, 8);
        MultiChunkStore multiChunkStore = new MultiChunkStore(chunkStore);

        VariableKeySizeMapChunkBackedKeyedStore[] onDiskIndexes = new VariableKeySizeMapChunkBackedKeyedStore[1];
        VariableKeySizeMapChunkBackedKeyedStore.Builder builder = new VariableKeySizeMapChunkBackedKeyedStore.Builder();
        builder.add(16, new PartitionedMapChunkBackedKeyedStore(
            new FileBackedMapChunkFactory(16, true, 8, false, 100, mapDirs),
            new FileBackedMapChunkFactory(16, true, 8, false, 100, swapDirs),
            multiChunkStore,
            4)); //TODO expose number of partitions
        onDiskIndexes[0] = builder.build();

        MiruOnDiskFieldIndex<EWAHCompressedBitmap> miruOnDiskFieldIndex = new MiruOnDiskFieldIndex<>(new MiruBitmapsEWAH(4), onDiskIndexes);
        miruOnDiskFieldIndex.bulkImport(tenantId, miruInMemoryFieldIndex);

        return new Object[][] {
            { miruInMemoryFieldIndex, importData, MiruBackingStorage.memory },
            { miruOnDiskFieldIndex, importData, MiruBackingStorage.disk }
        };
    }
}
