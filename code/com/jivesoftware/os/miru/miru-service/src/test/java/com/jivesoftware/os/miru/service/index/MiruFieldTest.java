package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.googlecode.javaewah.EWAHCompressedBitmap;
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
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskFieldIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruHybridFieldIndex;
import java.nio.file.Files;
import java.util.List;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class MiruFieldTest {

    @Test(dataProvider = "miruFieldDataProvider",
        enabled = true, description = "This test is disk dependent, disable if it flaps or becomes slow")
    public <BM> void getInvertedIndex(MiruBitmaps<BM> bitmaps, MiruFieldIndex<BM> fieldIndex, int fieldId, List<Integer> ids) throws Exception {
        for (int id : ids) {
            Optional<MiruInvertedIndex<BM>> optional = fieldIndex.get(fieldId, new MiruTermId(FilerIO.intBytes(id)));
            assertTrue(optional.isPresent());
            MiruInvertedIndex<BM> invertedIndex = optional.get();
            assertEquals(bitmaps.cardinality(invertedIndex.getIndex()), 1);
            assertTrue(bitmaps.isSet(invertedIndex.getIndex(), id));
        }
    }

    @Test(dataProvider = "miruFieldDataProvider",
        enabled = true, description = "This test is disk dependent, disable if it flaps or becomes slow")
    public <BM> void getInvertedIndexWithConsideration(MiruBitmaps<BM> bitmaps, MiruFieldIndex<BM> fieldIndex, int fieldId, List<Integer> ids)
        throws Exception {
        // this works because maxId = id in our termToIndex maps
        int median = ids.get(ids.size() / 2);

        for (int id : ids) {
            Optional<MiruInvertedIndex<BM>> optional = fieldIndex.get(fieldId, new MiruTermId(FilerIO.intBytes(id)), median);
            assertEquals(optional.isPresent(), id > median, "Should be " + optional.isPresent() + ": " + id + " > " + median);
        }
    }

    @DataProvider(name = "miruFieldDataProvider")
    public Object[][] miruFieldDataProvider() throws Exception {
        List<Integer> ids = Lists.newArrayList();
        MiruTenantId tenantId = new MiruTenantId(FilerIO.intBytes(1));
        MiruFieldDefinition fieldDefinition = new MiruFieldDefinition(0, "field1");

        @SuppressWarnings("unchecked")
        VariableKeySizeBytesObjectMapStore<byte[], MiruInvertedIndex<EWAHCompressedBitmap>>[] inMemIndexes = new VariableKeySizeBytesObjectMapStore[] {
            new VariableKeySizeBytesObjectMapStore<byte[], MiruInvertedIndex<EWAHCompressedBitmap>>(
                new BytesObjectMapStore[] {
                    new BytesObjectMapStore("16", 16, null,
                        new ByteBufferProviderBackedMapChunkFactory(16, true, 0, false, 10, new ByteBufferProvider("field-0", new HeapByteBufferFactory())),
                        PassThroughKeyMarshaller.INSTANCE)
                },
                PassThroughKeyMarshaller.INSTANCE)
        };
        MiruHybridFieldIndex<EWAHCompressedBitmap> hybridFieldIndex = new MiruHybridFieldIndex<>(new MiruBitmapsEWAH(2), inMemIndexes);

        for (int id = 0; id < 10; id++) {
            ids.add(id);
            hybridFieldIndex.index(fieldDefinition.fieldId, new MiruTermId(FilerIO.intBytes(id)), id);
        }

        String[] indexMapDirectories = new String[] {
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath(),
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath()
        };
        String[] indexSwapDirectories = new String[] {
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath(),
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath()
        };
        String[] chunkDirectories = new String[] {
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath(),
            Files.createTempDirectory(getClass().getSimpleName()).toFile().getAbsolutePath()
        };

        // 512 min size, times 10 field indexes
        MultiChunkStore multiChunkStore = new ChunkStoreInitializer().initializeMultiFileBacked(chunkDirectories, "data", 4, 5_120, false, 8);

        VariableKeySizeMapChunkBackedKeyedStore[] onDiskIndexes = new VariableKeySizeMapChunkBackedKeyedStore[1];
        VariableKeySizeMapChunkBackedKeyedStore.Builder builder = new VariableKeySizeMapChunkBackedKeyedStore.Builder();
        builder.add(16, new PartitionedMapChunkBackedKeyedStore(
            new FileBackedMapChunkFactory(16, true, 8, false, 100, indexMapDirectories),
            new FileBackedMapChunkFactory(16, true, 8, false, 100, indexSwapDirectories),
            multiChunkStore,
            4)); //TODO expose number of partitions
        onDiskIndexes[0] = builder.build();

        MiruOnDiskFieldIndex<EWAHCompressedBitmap> onDiskFieldIndex = new MiruOnDiskFieldIndex<>(new MiruBitmapsEWAH(2), onDiskIndexes);
        // need to export/import both the field and its index (a little strange)
        onDiskFieldIndex.bulkImport(tenantId, hybridFieldIndex);

        return new Object[][] {
            { new MiruBitmapsEWAH(2), hybridFieldIndex, fieldDefinition.fieldId, ids },
            { new MiruBitmapsEWAH(2), onDiskFieldIndex, fieldDefinition.fieldId, ids }
        };
    }
}
