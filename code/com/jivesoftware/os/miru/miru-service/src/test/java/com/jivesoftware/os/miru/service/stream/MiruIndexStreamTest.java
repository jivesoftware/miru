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
import com.jivesoftware.os.jive.utils.chunk.store.MultiChunkStore;
import com.jivesoftware.os.jive.utils.io.Filer;
import com.jivesoftware.os.jive.utils.io.RandomAccessFiler;
import com.jivesoftware.os.jive.utils.keyed.store.FileBackedKeyedStore;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.query.MiruActivityIndex;
import com.jivesoftware.os.miru.query.MiruActivityInternExtern;
import com.jivesoftware.os.miru.query.MiruAuthzIndex;
import com.jivesoftware.os.miru.query.MiruField;
import com.jivesoftware.os.miru.query.MiruFields;
import com.jivesoftware.os.miru.query.MiruInternalActivity;
import com.jivesoftware.os.miru.query.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.index.MiruFieldIndexKey;
import com.jivesoftware.os.miru.service.index.MiruFilerProvider;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import com.jivesoftware.os.miru.service.index.auth.VersionedAuthzExpression;
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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.fail;

public class MiruIndexStreamTest {

    private long initialChunkStoreSizeInBytes = 4096;
    private MiruFieldDefinition[] fieldDefinitions;
    private MiruSchema miruSchema;

    @BeforeMethod
    public void setUp() throws Exception {
        // Miru schema
        this.fieldDefinitions = new MiruFieldDefinition[] {
                new MiruFieldDefinition(0, "field1"),
                new MiruFieldDefinition(1, "field2"),
                new MiruFieldDefinition(2, "field3")
        };
        this.miruSchema = new MiruSchema(fieldDefinitions);
    }

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
    public void testIndexData(MiruTenantId tenantId, MiruIndexStream<EWAHCompressedBitmap> miruIndexStream, MiruActivityIndex miruActivityIndex,
            MiruFields<EWAHCompressedBitmap> miruFields, MiruAuthzIndex<EWAHCompressedBitmap> miruAuthzIndex, MiruBackingStorage miruBackingStorage)
            throws Exception {

        // First check existing data
        verifyFieldValues(tenantId, miruActivityIndex, miruFields, "field1", 0, 0);
        verifyAuthzValues(miruAuthzIndex, miruActivityIndex.get(tenantId, 0).authz, 0);
        verifyFieldValues(tenantId, miruActivityIndex, miruFields, "field2", 1, 1);
        verifyAuthzValues(miruAuthzIndex, miruActivityIndex.get(tenantId, 1).authz, 1);
        verifyFieldValues(tenantId, miruActivityIndex, miruFields, "field3", 2, 2);
        verifyAuthzValues(miruAuthzIndex, miruActivityIndex.get(tenantId, 2).authz, 2);

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
            verifyFieldValues(tenantId, miruActivityIndex, miruFields, "field1", 3, 0);
            verifyFieldValues(tenantId, miruActivityIndex, miruFields, "field2", 3, 1);
            verifyAuthzValues(miruAuthzIndex, miruActivityIndex.get(tenantId, 3).authz, 3);

            miruIndexStream.index(buildMiruActivity(tenantId, 5, new String[] { "uvwxy" },
                    ImmutableMap.of("field1", "field1Value1", "field3", "field3Value2")), 4);
            verifyFieldValues(tenantId, miruActivityIndex, miruFields, "field1", 4, 0);
            verifyFieldValues(tenantId, miruActivityIndex, miruFields, "field3", 4, 2);
            verifyAuthzValues(miruAuthzIndex, miruActivityIndex.get(tenantId, 4).authz, 4);
        }
    }

    private void verifyFieldValues(MiruTenantId tenantId, MiruActivityIndex miruActivityIndex, MiruFields<EWAHCompressedBitmap> fields, String fieldName,
            int activityId, int fieldId) throws Exception {
        MiruInternalActivity miruActivity = miruActivityIndex.get(tenantId, activityId);
        MiruField<EWAHCompressedBitmap> field = fields.getField(fieldId);

        MiruTermId[] fieldValues = miruActivity.fieldsValues[fieldId];
        for (MiruIBA fieldValue : fieldValues) {
            Optional<MiruInvertedIndex<EWAHCompressedBitmap>> invertedIndex = field.getInvertedIndex(new MiruTermId(fieldValue.getBytes()));
            assertNotNull(invertedIndex);
            assertTrue(invertedIndex.isPresent());
            EWAHCompressedBitmap fieldIndex = invertedIndex.get().getIndex();
            assertTrue(fieldIndex.get(activityId));
        }
    }

    private void verifyAuthzValues(MiruAuthzIndex<EWAHCompressedBitmap> miruAuthzIndex, String[] authzs, int activityId) throws Exception {
        MiruAuthzExpression miruAuthzExpression = new MiruAuthzExpression(Arrays.asList(authzs));

        EWAHCompressedBitmap compositeAuthz = miruAuthzIndex.getCompositeAuthz(miruAuthzExpression);
        assertTrue(compositeAuthz.get(activityId));
    }

    @DataProvider(name = "miruIndexStreamDataProvider")
    public Object[][] miruIndexStreamDataProvider() throws Exception {
        final MiruTenantId tenantId = new MiruTenantId("indexStreamTenant".getBytes());

        // Miru in-memory activity index
        MiruInMemoryActivityIndex miruInMemoryActivityIndex = new MiruInMemoryActivityIndex();

        // Miru in-memory fields
        MiruInMemoryIndex<EWAHCompressedBitmap> miruInMemoryIndex = new MiruInMemoryIndex<>(new MiruBitmapsEWAH(4));
        MiruInMemoryField<EWAHCompressedBitmap>[] inMemoryMiruFieldArray = buildInMemoryMiruFieldArray(miruInMemoryIndex);
        MiruFields<EWAHCompressedBitmap> inMemoryMiruFields = new MiruFields<>(inMemoryMiruFieldArray, miruInMemoryIndex);

        // Miru in-memory authz index
        MiruAuthzUtils<EWAHCompressedBitmap> miruAuthzUtils = new MiruAuthzUtils<>(new MiruBitmapsEWAH(4));
        MiruInMemoryAuthzIndex<EWAHCompressedBitmap> miruInMemoryAuthzIndex = new MiruInMemoryAuthzIndex<>(new MiruBitmapsEWAH(4), cache(miruAuthzUtils, 10));

        MiruInMemoryRemovalIndex<EWAHCompressedBitmap> miruInMemoryRemovalIndex = new MiruInMemoryRemovalIndex<>(new MiruBitmapsEWAH(4));

        MiruActivityInternExtern activityInterner = new MiruActivityInternExtern(Interners.<MiruIBA>newWeakInterner(), Interners.<MiruTermId>newWeakInterner(),
                Interners.<MiruTenantId>newWeakInterner(), Interners.<String>newWeakInterner());

        // Build in-memory index stream object
        MiruIndexStream<EWAHCompressedBitmap> miruInMemoryIndexStream = new MiruIndexStream<>(new MiruBitmapsEWAH(4), miruSchema, miruInMemoryActivityIndex,
                inMemoryMiruFields, miruInMemoryAuthzIndex, miruInMemoryRemovalIndex, activityInterner);

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
                new MultiChunkStore(new ChunkStoreInitializer().initialize(File.createTempFile("memmap", "chunk").getAbsolutePath(), 512, true)),
                new ObjectMapper());
        miruMemMappedActivityIndex.bulkImport(tenantId, miruInMemoryActivityIndex);

        // Miru on-disk fields
        MiruFields<EWAHCompressedBitmap> onDiskMiruFields = buildOnDiskMiruFields(tenantId, inMemoryMiruFieldArray, miruInMemoryIndex);

        Path chunksDir = Files.createTempDirectory("chunksAuthz");
        File chunks = new File(chunksDir.toFile(), "chunks.data");
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunks.getAbsolutePath(), initialChunkStoreSizeInBytes, false);
        MultiChunkStore multiChunkStore = new MultiChunkStore(chunkStore);
        // Miru on-disk authz index
        File authzMapDir = Files.createTempDirectory("mapAuthz").toFile();
        File authzSwapDir = Files.createTempDirectory("swapAuthz").toFile();
        MiruOnDiskAuthzIndex<EWAHCompressedBitmap> miruOnDiskAuthzIndex
                = new MiruOnDiskAuthzIndex<>(new MiruBitmapsEWAH(4), authzMapDir, authzSwapDir, multiChunkStore, cache(miruAuthzUtils, 10));
        miruOnDiskAuthzIndex.bulkImport(tenantId, miruInMemoryAuthzIndex);

        // Miru on-disk removal index
        File removalMapDir = Files.createTempDirectory("mapRemoval").toFile();
        File removalSwapDir = Files.createTempDirectory("swapRemoval").toFile();
        FileBackedKeyedStore removalStore
                = new FileBackedKeyedStore(removalMapDir.getAbsolutePath(), removalSwapDir.getAbsolutePath(), 1, 32, multiChunkStore, 32);
        MiruOnDiskRemovalIndex<EWAHCompressedBitmap> miruOnDiskRemovalIndex = new MiruOnDiskRemovalIndex<>(new MiruBitmapsEWAH(4), removalStore
                .get(new byte[] { 0 }));

        // Build on-disk index stream object
        MiruIndexStream<EWAHCompressedBitmap> miruOnDiskIndexStream = new MiruIndexStream<>(new MiruBitmapsEWAH(4),
                miruSchema, miruMemMappedActivityIndex, onDiskMiruFields, miruOnDiskAuthzIndex, miruOnDiskRemovalIndex, activityInterner);

        return new Object[][] {
                { tenantId, miruInMemoryIndexStream, miruInMemoryActivityIndex, inMemoryMiruFieldArray, miruInMemoryAuthzIndex, MiruBackingStorage.memory },
                { tenantId, miruOnDiskIndexStream, miruMemMappedActivityIndex, onDiskMiruFields, miruOnDiskAuthzIndex, MiruBackingStorage.disk }
        };
    }

    private MiruAuthzCache<EWAHCompressedBitmap> cache(MiruAuthzUtils<EWAHCompressedBitmap> miruAuthzUtils, int maximumSize) {
        Cache<VersionedAuthzExpression, EWAHCompressedBitmap> cache = CacheBuilder.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterAccess(1, TimeUnit.MINUTES)
                .build();
        MiruActivityInternExtern activityInterner = new MiruActivityInternExtern(Interners.<MiruIBA>newWeakInterner(), Interners
                .<MiruTermId>newWeakInterner(),
                Interners.<MiruTenantId>newWeakInterner(), Interners.<String>newWeakInterner());
        return new MiruAuthzCache<>(new MiruBitmapsEWAH(4), cache, activityInterner, miruAuthzUtils);
    }

    private MiruActivity buildMiruActivity(MiruTenantId tenantId, long time, String[] authz, Map<String, String> fields) {
        MiruActivity.Builder builder = new MiruActivity.Builder(tenantId, time, authz, 0);
        for (Map.Entry<String, String> field : fields.entrySet()) {
            builder.putFieldValue(field.getKey(), field.getValue());
        }
        return builder.build();
    }

    private MiruInMemoryField<EWAHCompressedBitmap>[] buildInMemoryMiruFieldArray(MiruInMemoryIndex<EWAHCompressedBitmap> miruInMemoryIndex) {
        return new MiruInMemoryField[] {
                new MiruInMemoryField<>(fieldDefinitions[0], Maps.<MiruTermId, MiruFieldIndexKey>newHashMap(), miruInMemoryIndex),
                new MiruInMemoryField<>(fieldDefinitions[1], Maps.<MiruTermId, MiruFieldIndexKey>newHashMap(), miruInMemoryIndex),
                new MiruInMemoryField<>(fieldDefinitions[2], Maps.<MiruTermId, MiruFieldIndexKey>newHashMap(), miruInMemoryIndex)
        };
    }

    private MiruFields<EWAHCompressedBitmap> buildOnDiskMiruFields(MiruTenantId tenantId, MiruInMemoryField<EWAHCompressedBitmap>[] inMemoryMiruFields,
            MiruInMemoryIndex<EWAHCompressedBitmap> miruInMemoryIndex) throws Exception {
        File mapDir = Files.createTempDirectory("mapFields").toFile();
        File swapDir = Files.createTempDirectory("swapFields").toFile();
        Path chunksDir = Files.createTempDirectory("chunksFields");
        File chunks = new File(chunksDir.toFile(), "chunks.data");
        ChunkStore chunkStore = new ChunkStoreInitializer().initialize(chunks.getAbsolutePath(), initialChunkStoreSizeInBytes, false);
        MultiChunkStore multiChunkStore = new MultiChunkStore(chunkStore);
        MiruOnDiskIndex<EWAHCompressedBitmap> miruOnDiskIndex = new MiruOnDiskIndex<>(new MiruBitmapsEWAH(4), mapDir, swapDir, multiChunkStore);
        miruOnDiskIndex.bulkImport(tenantId, miruInMemoryIndex);

        MiruField<EWAHCompressedBitmap>[] miruFieldArray = new MiruField[3];
        for (int i = 0; i < 3; i++) {
            MiruOnDiskField<EWAHCompressedBitmap> miruField = new MiruOnDiskField<>(fieldDefinitions[i], miruOnDiskIndex, mapDir);
            miruField.bulkImport(tenantId, inMemoryMiruFields[i]);
            miruFieldArray[i] = miruField;
        }

        return new MiruFields<>(miruFieldArray, miruInMemoryIndex);
    }
}
