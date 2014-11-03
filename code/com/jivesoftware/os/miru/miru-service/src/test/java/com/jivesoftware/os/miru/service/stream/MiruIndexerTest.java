package com.jivesoftware.os.miru.service.stream;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Interners;
import com.google.common.util.concurrent.MoreExecutors;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.chunk.store.MultiChunkStore;
import com.jivesoftware.os.filer.io.ByteBufferProvider;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.keyed.store.PartitionedMapChunkBackedKeyedStore;
import com.jivesoftware.os.filer.keyed.store.VariableKeySizeMapChunkBackedKeyedStore;
import com.jivesoftware.os.filer.map.store.ByteBufferProviderBackedMapChunkFactory;
import com.jivesoftware.os.filer.map.store.BytesObjectMapStore;
import com.jivesoftware.os.filer.map.store.FileBackedMapChunkFactory;
import com.jivesoftware.os.filer.map.store.PassThroughKeyMarshaller;
import com.jivesoftware.os.filer.map.store.VariableKeySizeBytesObjectMapStore;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityInternExtern;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.index.MiruInternalActivityMarshaller;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzCache;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import com.jivesoftware.os.miru.service.index.auth.VersionedAuthzExpression;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskAuthzIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskFieldIndex;
import com.jivesoftware.os.miru.service.index.disk.MiruOnDiskRemovalIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruHybridActivityIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruHybridFieldIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryAuthzIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryRemovalIndex;
import com.jivesoftware.os.miru.service.locator.MiruResourcePartitionIdentifier;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.MapChunkFactoryTestUtil.createFileBackedMapChunkFactory;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.fail;

public class MiruIndexerTest {

    private long initialChunkStoreSizeInBytes = 4_096;

    @Test(dataProvider = "miruIndexContextDataProvider")
    public void testIndexData(MiruTenantId tenantId,
        MiruContext<EWAHCompressedBitmap> context,
        MiruIndexer<EWAHCompressedBitmap> miruIndexer,
        MiruBackingStorage miruBackingStorage)
        throws Exception {

        // First check existing data
        verifyFieldValues(tenantId, context, 0, 0);
        verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, 0).authz, 0);
        verifyFieldValues(tenantId, context, 1, 1);
        verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, 1).authz, 1);
        verifyFieldValues(tenantId, context, 2, 2);
        verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, 2).authz, 2);

        if (miruBackingStorage.equals(MiruBackingStorage.disk)) {
            try {
                miruIndexer.index(
                    context,
                    Arrays.asList(new MiruActivityAndId<>(
                        buildMiruActivity(tenantId, 4, new String[0], ImmutableMap.of("field1", "field1Value2", "field2", "field2Value2")),
                        4)),
                    MoreExecutors.sameThreadExecutor());
                fail("This index type is supposed to be readOnly");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof UnsupportedOperationException);
            }
        } else {
            // Next add new data and check it
            miruIndexer.index(
                context,
                Arrays.asList(new MiruActivityAndId<>(buildMiruActivity(tenantId, 4, new String[] { "pqrst" },
                    ImmutableMap.of("field1", "field1Value2", "field2", "field2Value2")), 3)),
                MoreExecutors.sameThreadExecutor());
            verifyFieldValues(tenantId, context, 3, 0);
            verifyFieldValues(tenantId, context, 3, 1);
            verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, 3).authz, 3);

            miruIndexer.index(
                context,
                Arrays.asList(new MiruActivityAndId<>(
                    buildMiruActivity(tenantId, 5, new String[] { "uvwxy" }, ImmutableMap.of("field1", "field1Value1", "field3", "field3Value2")),
                    4)),
                MoreExecutors.sameThreadExecutor());
            verifyFieldValues(tenantId, context, 4, 0);
            verifyFieldValues(tenantId, context, 4, 2);
            verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, 4).authz, 4);
        }
    }

    private void verifyFieldValues(MiruTenantId tenantId, MiruContext<EWAHCompressedBitmap> context, int activityId, int fieldId) throws Exception {

        MiruInternalActivity miruActivity = context.getActivityIndex().get(tenantId, activityId);

        MiruTermId[] fieldValues = miruActivity.fieldsValues[fieldId];
        for (MiruIBA fieldValue : fieldValues) {
            Optional<MiruInvertedIndex<EWAHCompressedBitmap>> invertedIndex = context.getFieldIndex().get(fieldId, new MiruTermId(fieldValue.getBytes()));
            assertNotNull(invertedIndex);
            assertTrue(invertedIndex.isPresent());
            EWAHCompressedBitmap bitmap = invertedIndex.get().getIndex();
            assertTrue(bitmap.get(activityId));
        }
    }

    private void verifyAuthzValues(MiruAuthzIndex<EWAHCompressedBitmap> miruAuthzIndex, String[] authzs, int activityId) throws Exception {
        MiruAuthzExpression miruAuthzExpression = new MiruAuthzExpression(Arrays.asList(authzs));

        EWAHCompressedBitmap compositeAuthz = miruAuthzIndex.getCompositeAuthz(miruAuthzExpression);
        assertTrue(compositeAuthz.get(activityId));
    }

    @DataProvider(name = "miruIndexContextDataProvider")
    public Object[][] miruIndexContextDataProvider() throws Exception {
        final MiruTenantId tenantId = new MiruTenantId("indexContextTenant".getBytes());

        MiruFieldDefinition[] fieldDefinitions = new MiruFieldDefinition[] {
            new MiruFieldDefinition(0, "field1"),
            new MiruFieldDefinition(1, "field2"),
            new MiruFieldDefinition(2, "field3")
        };
        MiruSchema miruSchema = new MiruSchema(fieldDefinitions);
        MiruIndexer<EWAHCompressedBitmap> miruIndexer = new MiruIndexer<>(new MiruBitmapsEWAH(4));

        // Miru in-memory activity index
        MiruHybridActivityIndex miruInMemoryActivityIndex = new MiruHybridActivityIndex(
            new PartitionedMapChunkBackedKeyedStore(
                createFileBackedMapChunkFactory("activity", 4, false, 8, false, 100, 2),
                createFileBackedMapChunkFactory("activity", 4, false, 8, false, 100, 2),
                new MultiChunkStore(new ChunkStoreInitializer().create(new Object(), new ByteBufferProvider("chunks", new HeapByteBufferFactory()), 512, true)),
                24),
            new MiruInternalActivityMarshaller());

        // Miru in-memory fields
        @SuppressWarnings("unchecked")
        VariableKeySizeBytesObjectMapStore<byte[], MiruInvertedIndex<EWAHCompressedBitmap>>[] indexes = new VariableKeySizeBytesObjectMapStore[] {
            new VariableKeySizeBytesObjectMapStore<byte[], MiruInvertedIndex<EWAHCompressedBitmap>>(
                new BytesObjectMapStore[] {
                    new BytesObjectMapStore<byte[], MiruInvertedIndex<EWAHCompressedBitmap>>("16", 16, null,
                        new ByteBufferProviderBackedMapChunkFactory(16, true, 0, false, 10, new ByteBufferProvider("field-0", new HeapByteBufferFactory())),
                        PassThroughKeyMarshaller.INSTANCE)
                },
                PassThroughKeyMarshaller.INSTANCE)
        };
        MiruHybridFieldIndex<EWAHCompressedBitmap> miruInMemoryFieldIndex = new MiruHybridFieldIndex<>(new MiruBitmapsEWAH(4), indexes);

        // Miru in-memory authz index
        MiruAuthzUtils<EWAHCompressedBitmap> miruAuthzUtils = new MiruAuthzUtils<>(new MiruBitmapsEWAH(4));
        MiruInMemoryAuthzIndex<EWAHCompressedBitmap> miruInMemoryAuthzIndex = new MiruInMemoryAuthzIndex<>(new MiruBitmapsEWAH(4), cache(miruAuthzUtils, 10));

        MiruInMemoryRemovalIndex<EWAHCompressedBitmap> miruInMemoryRemovalIndex = new MiruInMemoryRemovalIndex<>(new MiruBitmapsEWAH(4));

        MiruActivityInternExtern activityInterner = new MiruActivityInternExtern(Interners.<MiruIBA>newWeakInterner(), Interners.<MiruTermId>newWeakInterner(),
            Interners.<MiruTenantId>newWeakInterner(), Interners.<String>newWeakInterner());

        // Build in-memory index stream object
        MiruActivity miruActivity1 = buildMiruActivity(tenantId, 1, new String[] { "abcde" }, ImmutableMap.of("field1", "field1Value1"));
        MiruActivity miruActivity2 = buildMiruActivity(tenantId, 2, new String[] { "fghij" }, ImmutableMap.of("field2", "field2Value1"));
        MiruActivity miruActivity3 = buildMiruActivity(tenantId, 3, new String[] { "klmno" }, ImmutableMap.of("field3", "field3Value1"));

        MiruContext<EWAHCompressedBitmap> miruInMemoryContext = new MiruContext<>(miruSchema, null, miruInMemoryActivityIndex, miruInMemoryFieldIndex,
            miruInMemoryAuthzIndex, miruInMemoryRemovalIndex, null, null, null, activityInterner, null, Optional.<MultiChunkStore>absent(),
            Optional.<MiruResourcePartitionIdentifier>absent());

        // Index initial activities
        miruIndexer.index(miruInMemoryContext,
            Arrays.asList(
                new MiruActivityAndId<>(miruActivity1, 0),
                new MiruActivityAndId<>(miruActivity2, 1),
                new MiruActivityAndId<>(miruActivity3, 2)),
            MoreExecutors.sameThreadExecutor());

        String hybridChunksDir = Files.createTempDirectory("chunk").toFile().getAbsolutePath();
        MiruHybridActivityIndex miruOnDiskActivityIndex = new MiruHybridActivityIndex(
            new PartitionedMapChunkBackedKeyedStore(
                createFileBackedMapChunkFactory("activity", 4, false, 8, false, 100, 2),
                createFileBackedMapChunkFactory("activity", 4, false, 8, false, 100, 2),
                new MultiChunkStore(new ChunkStoreInitializer().initialize(hybridChunksDir, "memmap", 512, true)),
                24),
            new MiruInternalActivityMarshaller());

        miruOnDiskActivityIndex.bulkImport(tenantId, miruInMemoryActivityIndex);

        // Miru on-disk fields
        MiruOnDiskFieldIndex<EWAHCompressedBitmap> miruOnDiskFieldIndex = buildOnDiskMiruFieldIndex();
        miruOnDiskFieldIndex.bulkImport(tenantId, miruInMemoryFieldIndex);


        String[] authzMapDirs = {
            Files.createTempDirectory("authz").toFile().getAbsolutePath(),
            Files.createTempDirectory("authz").toFile().getAbsolutePath()
        };
        String[] authzSwapDirs = {
            Files.createTempDirectory("authz").toFile().getAbsolutePath(),
            Files.createTempDirectory("authz").toFile().getAbsolutePath()
        };
        String[] hybridChunksDirs = new String[] {
            Files.createTempDirectory("chunk").toFile().getAbsolutePath(),
            Files.createTempDirectory("chunk").toFile().getAbsolutePath()
        };
        MultiChunkStore hybridMultiChunkStore = new ChunkStoreInitializer()
            .initializeMultiFileBacked(hybridChunksDirs, "data", 4, initialChunkStoreSizeInBytes, false);
        // Miru on-disk authz index
        MiruOnDiskAuthzIndex<EWAHCompressedBitmap> miruOnDiskAuthzIndex = new MiruOnDiskAuthzIndex<>(
            new MiruBitmapsEWAH(4),
            authzMapDirs,
            authzSwapDirs,
            hybridMultiChunkStore,
            cache(miruAuthzUtils, 10));
        miruOnDiskAuthzIndex.bulkImport(tenantId, miruInMemoryAuthzIndex);

        // Miru on-disk removal index
        PartitionedMapChunkBackedKeyedStore removalStore = new PartitionedMapChunkBackedKeyedStore(
            createFileBackedMapChunkFactory("mapRemoval", 1, false, 8, false, 32, 2),
            createFileBackedMapChunkFactory("swapRemoval", 1, false, 8, false, 32, 2),
            hybridMultiChunkStore,
            4);
        MiruOnDiskRemovalIndex<EWAHCompressedBitmap> miruOnDiskRemovalIndex = new MiruOnDiskRemovalIndex<>(
            new MiruBitmapsEWAH(4),
            removalStore.get(new byte[] { 0 }, 32));

        MiruContext<EWAHCompressedBitmap> miruOnDiskContext = new MiruContext<>(miruSchema, null, miruOnDiskActivityIndex, miruOnDiskFieldIndex,
            miruOnDiskAuthzIndex, miruOnDiskRemovalIndex, null, null, null, activityInterner, null, Optional.<MultiChunkStore>absent(),
            Optional.<MiruResourcePartitionIdentifier>absent());

        // Build on-disk index stream object

        return new Object[][] {
            { tenantId, miruInMemoryContext, miruIndexer, MiruBackingStorage.memory },
            { tenantId, miruOnDiskContext, miruIndexer, MiruBackingStorage.disk }
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

    private MiruOnDiskFieldIndex<EWAHCompressedBitmap> buildOnDiskMiruFieldIndex() throws Exception {

        String[] mapDirs = new String[] {
            Files.createTempDirectory("mapFields").toFile().getAbsolutePath(),
            Files.createTempDirectory("mapFields").toFile().getAbsolutePath()
        };
        String[] swapDirs = new String[] {
            Files.createTempDirectory("swapFields").toFile().getAbsolutePath(),
            Files.createTempDirectory("swapFields").toFile().getAbsolutePath()
        };
        String[] chunksDirs = new String[] {
            Files.createTempDirectory("chunksFields").toFile().getAbsolutePath(),
            Files.createTempDirectory("chunksFields").toFile().getAbsolutePath()
        };
        MultiChunkStore multiChunkStore = new ChunkStoreInitializer().initializeMultiFileBacked(chunksDirs, "data", 4, initialChunkStoreSizeInBytes, false);

        VariableKeySizeMapChunkBackedKeyedStore[] onDiskIndexes = new VariableKeySizeMapChunkBackedKeyedStore[1];
        VariableKeySizeMapChunkBackedKeyedStore.Builder builder = new VariableKeySizeMapChunkBackedKeyedStore.Builder();
        builder.add(16, new PartitionedMapChunkBackedKeyedStore(
            new FileBackedMapChunkFactory(16, true, 8, false, 100, mapDirs),
            new FileBackedMapChunkFactory(16, true, 8, false, 100, swapDirs),
            multiChunkStore,
            4)); //TODO expose number of partitions
        onDiskIndexes[0] = builder.build();

        return new MiruOnDiskFieldIndex<>(new MiruBitmapsEWAH(4), onDiskIndexes);
    }
}
