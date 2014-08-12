package com.jivesoftware.os.miru.service.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Interners;
import com.google.common.collect.Maps;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStore;
import com.jivesoftware.os.jive.utils.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.jive.utils.io.Filer;
import com.jivesoftware.os.jive.utils.io.RandomAccessFiler;
import com.jivesoftware.os.jive.utils.keyed.store.FileBackedKeyedStore;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.service.index.MiruActivityIndex;
import com.jivesoftware.os.miru.service.index.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.service.index.MiruAuthzUtils;
import com.jivesoftware.os.miru.service.index.MiruField;
import com.jivesoftware.os.miru.service.index.MiruFieldIndexKey;
import com.jivesoftware.os.miru.service.index.MiruFields;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import com.jivesoftware.os.miru.service.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.index.VersionedAuthzExpression;
import com.jivesoftware.os.miru.service.index.disk.MiruMemMappedActivityIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskAuthzIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskField;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskRemovalIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryActivityIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryAuthzIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryField;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryRemovalIndex;
import com.jivesoftware.os.miru.service.schema.MiruSchema;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.fail;

public class MiruIndexStreamTest {

    private long initialChunkStoreSizeInBytes = 4096;

    @Test(dataProvider = "miruIndexStreamDataProvider")
    public void testSizeInBytes(MiruTenantId tenantId, MiruIndexStream miruIndexStream, MiruActivityIndex miruActivityIndex, MiruFields miruFields,
        MiruAuthzIndex miruAuthzIndex, MiruBackingStorage miruBackingStorage) throws Exception {

        long sizeInBytes = miruFields.sizeInMemory() + miruFields.sizeOnDisk() + miruAuthzIndex.sizeInMemory() + miruAuthzIndex.sizeOnDisk();
        if (miruBackingStorage.equals(MiruBackingStorage.memory)) {
            // 52 bytes per field (3 activities) + 42 bytes per unique authz (3 unique authz values)
            long expectedSizeInBytes = (52 * 3) + (42 * 3);
            assertEquals(sizeInBytes, expectedSizeInBytes);
        } else if (miruBackingStorage.equals(MiruBackingStorage.disk)) {
            // See MapStore.cost() for more information. FileBackedMemMappedByteBufferFactory.allocate() adds the extra byte
            long initialAuthzMapStoreSizeInBytes = 3547;
            long authzSizeInBytes = initialAuthzMapStoreSizeInBytes;

            // See MapStore.cost() for more information. FileBackedMemMappedByteBufferFactory.allocate() adds the extra byte
            long initialFieldIndexMapStoreSizeInBytes = 16;
            long initialTermIndexSizeInBytes = 3547;
            long fieldIndexSizeInBytes = initialFieldIndexMapStoreSizeInBytes + initialTermIndexSizeInBytes;

            // 1 authz index, 3 field indexes
            long expectedSizeInBytes = authzSizeInBytes + (3 * fieldIndexSizeInBytes);
            assertEquals(sizeInBytes, expectedSizeInBytes);
        }
    }

    @Test(dataProvider = "miruIndexStreamDataProvider")
    public void testIndexSize(MiruTenantId tenantId, MiruIndexStream miruIndexStream, MiruActivityIndex miruActivityIndex, MiruFields miruFields,
        MiruAuthzIndex miruAuthzIndex, MiruBackingStorage miruBackingStorage) throws Exception {

        if (miruBackingStorage.equals(MiruBackingStorage.disk)) {
            try {
                miruIndexStream.index(buildMiruActivity(tenantId, 4, new String[0], ImmutableMap.of("field1", "field1Value2", "field2", "field2Value2")), 4);
                fail("This index type is supposed to be readOnly");
            } catch (UnsupportedOperationException e) {
                return;
            }
        } else {
            long expectedSizeInBytes = (52 * 3) + (42 * 3); // See testSizeInBytes();

            miruIndexStream.index(buildMiruActivity(tenantId, 4, new String[] { "uvwxy" },
                ImmutableMap.of("field1", "field1Value2", "field2", "field2Value2")), 4);
            expectedSizeInBytes += 52 + 52 + 42; // 2 new fields, 1 new authz value
            long sizeInBytes = miruFields.sizeInMemory() + miruFields.sizeOnDisk() + miruAuthzIndex.sizeInMemory() + miruAuthzIndex.sizeOnDisk();
            assertEquals(sizeInBytes, expectedSizeInBytes);

            miruIndexStream.index(buildMiruActivity(tenantId, 5, new String[] { "pqrst" },
                ImmutableMap.of("field1", "field1Value1", "field3", "field3Value2")), 5);
            expectedSizeInBytes += 52 + 0 + 42; // 1 new field, 1 existing field and 1 new authz value
            sizeInBytes = miruFields.sizeInMemory() + miruFields.sizeOnDisk() + miruAuthzIndex.sizeInMemory() + miruAuthzIndex.sizeOnDisk();
            assertEquals(sizeInBytes, expectedSizeInBytes);
        }
    }

    @Test(dataProvider = "miruIndexStreamDataProvider")
    public void testIndexData(MiruTenantId tenantId, MiruIndexStream miruIndexStream, MiruActivityIndex miruActivityIndex, MiruFields miruFields,
        MiruAuthzIndex miruAuthzIndex, MiruBackingStorage miruBackingStorage) throws Exception {

        // First check existing data
        verifyFieldValues(miruActivityIndex, miruFields, "field1", 0, 0);
        verifyAuthzValues(miruAuthzIndex, miruActivityIndex.get(0).authz, 0);
        verifyFieldValues(miruActivityIndex, miruFields, "field2", 1, 1);
        verifyAuthzValues(miruAuthzIndex, miruActivityIndex.get(1).authz, 1);
        verifyFieldValues(miruActivityIndex, miruFields, "field3", 2, 2);
        verifyAuthzValues(miruAuthzIndex, miruActivityIndex.get(2).authz, 2);

        if (miruBackingStorage.equals(MiruBackingStorage.disk)) {
            try {
                miruIndexStream.index(buildMiruActivity(tenantId, 4, new String[0], ImmutableMap.of("field1", "field1Value2", "field2", "field2Value2")), 4);
                fail("This index type is supposed to be readOnly");
            } catch (UnsupportedOperationException e) {
                return;
            }
        } else {
            // Next add new data and check it
            miruIndexStream.index(buildMiruActivity(tenantId, 4, new String[] { "pqrst" },
                ImmutableMap.of("field1", "field1Value2", "field2", "field2Value2")), 3);
            verifyFieldValues(miruActivityIndex, miruFields, "field1", 3, 0);
            verifyFieldValues(miruActivityIndex, miruFields, "field2", 3, 1);
            verifyAuthzValues(miruAuthzIndex, miruActivityIndex.get(3).authz, 3);

            miruIndexStream.index(buildMiruActivity(tenantId, 5, new String[] { "uvwxy" },
                ImmutableMap.of("field1", "field1Value1", "field3", "field3Value2")), 4);
            verifyFieldValues(miruActivityIndex, miruFields, "field1", 4, 0);
            verifyFieldValues(miruActivityIndex, miruFields, "field3", 4, 2);
            verifyAuthzValues(miruAuthzIndex, miruActivityIndex.get(4).authz, 4);
        }
    }

    private void verifyFieldValues(MiruActivityIndex miruActivityIndex, MiruFields fields, String fieldName, int activityId, int fieldId) throws Exception {
        MiruActivity miruActivity = miruActivityIndex.get(activityId);
        MiruField field = fields.getField(fieldId);

        MiruTermId[] fieldValues = miruActivity.fieldsValues.get(fieldName);
        for (MiruIBA fieldValue : fieldValues) {
            Optional<MiruInvertedIndex> invertedIndex = field.getInvertedIndex(new MiruTermId(fieldValue.getBytes()));
            assertNotNull(invertedIndex);
            assertTrue(invertedIndex.isPresent());
            EWAHCompressedBitmap fieldIndex = invertedIndex.get().getIndex();
            assertTrue(fieldIndex.get(activityId));
        }
    }

    private void verifyAuthzValues(MiruAuthzIndex miruAuthzIndex, String[] authzs, int activityId) throws Exception {
        MiruAuthzExpression miruAuthzExpression = new MiruAuthzExpression(Arrays.asList(authzs));

        EWAHCompressedBitmap compositeAuthz = miruAuthzIndex.getCompositeAuthz(miruAuthzExpression);
        assertTrue(compositeAuthz.get(activityId));
    }

    @DataProvider(name = "miruIndexStreamDataProvider")
    public Object[][] miruIndexStreamDataProvider() throws Exception {
        final MiruTenantId tenantId = new MiruTenantId("indexStreamTenant".getBytes());

        // Miru schema
        MiruSchema miruSchema = new MiruSchema(ImmutableMap.<String, Integer>of(
            "field1", 0,
            "field2", 1,
            "field3", 2
        ));

        // Miru in-memory activity index
        MiruInMemoryActivityIndex miruInMemoryActivityIndex = new MiruInMemoryActivityIndex();

        // Miru in-memory fields
        MiruInMemoryIndex miruInMemoryIndex = new MiruInMemoryIndex();
        MiruFields inMemoryMiruFields = buildInMemoryMiruFields(miruInMemoryIndex);

        // Miru in-memory authz index
        MiruAuthzUtils miruAuthzUtils = new MiruAuthzUtils(8);
        MiruInMemoryAuthzIndex miruInMemoryAuthzIndex = new MiruInMemoryAuthzIndex(cache(miruAuthzUtils, 10));

        MiruInMemoryRemovalIndex miruInMemoryRemovalIndex = new MiruInMemoryRemovalIndex(new EWAHCompressedBitmap());

        MiruActivityInterner activityInterner = new MiruActivityInterner(Interners.<MiruIBA>newWeakInterner(), Interners.<MiruTermId>newWeakInterner(),
            Interners.<MiruTenantId>newWeakInterner(), Interners.<String>newWeakInterner());

        // Build in-memory index stream object
        MiruIndexStream miruInMemoryIndexStream = new MiruIndexStream(miruSchema, miruInMemoryActivityIndex, inMemoryMiruFields, miruInMemoryAuthzIndex,
            miruInMemoryRemovalIndex, activityInterner);

        MiruActivity miruActivity1 = buildMiruActivity(tenantId, 1, new String[] { "abcde" }, ImmutableMap.of("field1", "field1Value1"));
        MiruActivity miruActivity2 = buildMiruActivity(tenantId, 2, new String[] { "fghij" }, ImmutableMap.of("field2", "field2Value1"));
        MiruActivity miruActivity3 = buildMiruActivity(tenantId, 3, new String[] { "klmno" }, ImmutableMap.of("field3", "field3Value1"));

        // Index initial activities
        miruInMemoryIndexStream.index(miruActivity1, 0);
        miruInMemoryIndexStream.index(miruActivity2, 1);
        miruInMemoryIndexStream.index(miruActivity3, 2);

        // Miru mem-mapped activity index
        final File memMap = File.createTempFile("memmap", "activityindex");
        MiruMemMappedActivityIndex miruMemMappedActivityIndex = new MiruMemMappedActivityIndex(
            new MiruFilerProvider() {
                @Override
                public File getBackingFile() {
                    return memMap;
                }

                @Override
                public Filer getFiler(long length) throws IOException {
                    return new RandomAccessFiler(memMap, "rw");
                }
            },
            Files.createTempDirectory("memmap").toFile(),
            Files.createTempDirectory("memmap").toFile(),
            new ChunkStoreInitializer().initialize(File.createTempFile("memmap", "chunk").getAbsolutePath(), 512, true),
            new ObjectMapper());
        miruMemMappedActivityIndex.bulkImport(miruInMemoryActivityIndex);

        // Miru on-disk fields
        MiruFields onDiskMiruFields = buildOnDiskMiruFields(inMemoryMiruFields, miruInMemoryIndex);

        Path chunksDir = Files.createTempDirectory("chunksAuthz");
        File chunks = new File(chunksDir.toFile(), "chunks.data");
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunks.getAbsolutePath(), initialChunkStoreSizeInBytes, false);

        // Miru on-disk authz index
        File authzMapDir = Files.createTempDirectory("mapAuthz").toFile();
        File authzSwapDir = Files.createTempDirectory("swapAuthz").toFile();
        MiruOnDiskAuthzIndex miruOnDiskAuthzIndex = new MiruOnDiskAuthzIndex(authzMapDir, authzSwapDir, chunkStore, cache(miruAuthzUtils, 10));
        miruOnDiskAuthzIndex.bulkImport(miruInMemoryAuthzIndex);

        // Miru on-disk removal index
        File removalMapDir = Files.createTempDirectory("mapRemoval").toFile();
        File removalSwapDir = Files.createTempDirectory("swapRemoval").toFile();
        FileBackedKeyedStore removalStore = new FileBackedKeyedStore(removalMapDir.getAbsolutePath(), removalSwapDir.getAbsolutePath(), 1, 32, chunkStore, 32);
        MiruOnDiskRemovalIndex miruOnDiskRemovalIndex = new MiruOnDiskRemovalIndex(removalStore.get(new byte[] { 0 }));

        // Build on-disk index stream object
        MiruIndexStream miruOnDiskIndexStream = new MiruIndexStream(
            miruSchema, miruMemMappedActivityIndex, onDiskMiruFields, miruOnDiskAuthzIndex, miruOnDiskRemovalIndex, activityInterner);

        return new Object[][] {
            { tenantId, miruInMemoryIndexStream, miruInMemoryActivityIndex, inMemoryMiruFields, miruInMemoryAuthzIndex, MiruBackingStorage.memory },
            { tenantId, miruOnDiskIndexStream, miruMemMappedActivityIndex, onDiskMiruFields, miruOnDiskAuthzIndex, MiruBackingStorage.disk }
        };
    }

    private MiruAuthzCache cache(MiruAuthzUtils miruAuthzUtils, int maximumSize) {
        Cache<VersionedAuthzExpression, EWAHCompressedBitmap> cache = CacheBuilder.newBuilder()
            .maximumSize(maximumSize)
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .build();
        return new MiruAuthzCache(cache, Interners.<String>newWeakInterner(), miruAuthzUtils);
    }

    private MiruActivity buildMiruActivity(MiruTenantId tenantId, long time, String[] authz, Map<String, String> fields) {
        MiruActivity.Builder builder = new MiruActivity.Builder(tenantId, time, authz, 0);
        for (Map.Entry<String, String> field : fields.entrySet()) {
            builder.putFieldValue(field.getKey(), field.getValue());
        }
        return builder.build();
    }

    private MiruFields buildInMemoryMiruFields(MiruInMemoryIndex miruInMemoryIndex) {
        MiruField[] miruFieldArray = new MiruField[] {
            new MiruInMemoryField(0, Maps.<MiruTermId, MiruFieldIndexKey>newHashMap(), miruInMemoryIndex),
            new MiruInMemoryField(1, Maps.<MiruTermId, MiruFieldIndexKey>newHashMap(), miruInMemoryIndex),
            new MiruInMemoryField(2, Maps.<MiruTermId, MiruFieldIndexKey>newHashMap(), miruInMemoryIndex)
        };

        return new MiruFields(miruFieldArray, miruInMemoryIndex);
    }

    private MiruFields buildOnDiskMiruFields(MiruFields inMemoryMiruFields, MiruInMemoryIndex miruInMemoryIndex) throws Exception {
        File mapDir = Files.createTempDirectory("mapFields").toFile();
        File swapDir = Files.createTempDirectory("swapFields").toFile();
        Path chunksDir = Files.createTempDirectory("chunksFields");
        File chunks = new File(chunksDir.toFile(), "chunks.data");
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunks.getAbsolutePath(), initialChunkStoreSizeInBytes, false);
        MiruOnDiskIndex miruOnDiskIndex = new MiruOnDiskIndex(mapDir, swapDir, chunkStore);
        miruOnDiskIndex.bulkImport(miruInMemoryIndex);

        MiruField[] miruFieldArray = new MiruField[3];
        for (int i = 0; i < 3; i++) {
            MiruOnDiskField miruField = new MiruOnDiskField(i, miruOnDiskIndex, mapDir);
            miruField.bulkImport(((MiruInMemoryField) inMemoryMiruFields.getField(i)));
            miruFieldArray[i] = miruField;
        }

        return new MiruFields(miruFieldArray, miruInMemoryIndex);
    }
}
