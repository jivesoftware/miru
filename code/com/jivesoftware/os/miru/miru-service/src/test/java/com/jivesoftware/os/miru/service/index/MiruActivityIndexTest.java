package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.ByteBufferProvider;
import com.jivesoftware.os.filer.io.DirectByteBufferFactory;
import com.jivesoftware.os.filer.io.HeapByteBufferFactory;
import com.jivesoftware.os.filer.keyed.store.PartitionedMapChunkBackedKeyedStore;
import com.jivesoftware.os.filer.map.store.ByteBufferProviderBackedMapChunkFactory;
import com.jivesoftware.os.filer.map.store.MapChunkFactory;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.service.index.memory.MiruHybridActivityIndex;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.MapChunkFactoryTestUtil.createFileBackedMapChunkFactory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class MiruActivityIndexTest {

    MiruSchema schema = new MiruSchema(
        new MiruFieldDefinition(0, "a"),
        new MiruFieldDefinition(1, "b"),
        new MiruFieldDefinition(2, "c"),
        new MiruFieldDefinition(3, "d"),
        new MiruFieldDefinition(4, "e"));

    @Test(dataProvider = "miruActivityIndexDataProvider")
    public void testSetActivity(MiruActivityIndex miruActivityIndex, boolean throwsUnsupportedExceptionOnSet) throws Exception {
        MiruInternalActivity miruActivity = buildMiruActivity(new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes()), 1, new String[0], 5);
        try {
            miruActivityIndex.setAndReady(Arrays.asList(new MiruActivityAndId<>(miruActivity, 0)));
            if (throwsUnsupportedExceptionOnSet) {
                fail("This implementation of the MiruActivityIndex should have thrown an UnsupportedOperationException");
            }
        } catch (UnsupportedOperationException e) {
            if (!throwsUnsupportedExceptionOnSet) {
                fail("This implementation of the MiruActivityIndex shouldn't have thrown an UnsupportedOperationException", e);
            }
        }
    }

    @Test(dataProvider = "miruActivityIndexDataProvider")
    public void testSetActivityOutOfBounds(MiruActivityIndex miruActivityIndex, boolean throwsUnsupportedExceptionOnSet) throws Exception {
        MiruInternalActivity miruActivity = buildMiruActivity(new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes()), 1, new String[0], 5);
        try {
            miruActivityIndex.setAndReady(Arrays.asList(new MiruActivityAndId<>(miruActivity, -1)));
            if (throwsUnsupportedExceptionOnSet) {
                fail("This implementation of the MiruActivityIndex should have thrown an UnsupportedOperationException");
            }
            fail("Expected an IllegalArgumentException but never got it");
        } catch (UnsupportedOperationException e) {
            if (!throwsUnsupportedExceptionOnSet) {
                fail("This implementation of the MiruActivityIndex shouldn't have thrown an UnsupportedOperationException", e);
            }
        } catch (IllegalArgumentException e) {
            // We want to get here, fall through
        }
    }

    @Test(dataProvider = "miruActivityIndexDataProviderWithData")
    public void testGetActivity(MiruActivityIndex miruActivityIndex, MiruInternalActivity[] expectedActivities) {
        assertTrue(expectedActivities.length == 3);
        assertEquals(miruActivityIndex.get(expectedActivities[0].tenantId, 0), expectedActivities[0]);
        assertEquals(miruActivityIndex.get(expectedActivities[1].tenantId, 1), expectedActivities[1]);
        assertEquals(miruActivityIndex.get(expectedActivities[2].tenantId, 2), expectedActivities[2]);
    }

    @Test(dataProvider = "miruActivityIndexDataProviderWithData", expectedExceptions = IllegalArgumentException.class)
    public void testGetActivityOverCapacity(MiruActivityIndex miruActivityIndex, MiruInternalActivity[] expectedActivities) {
        miruActivityIndex.get(null, expectedActivities.length); // This should throw an exception
    }

    @DataProvider(name = "miruActivityIndexDataProvider")
    public Object[][] miruActivityIndexDataProvider() throws Exception {
        String[] byteBufferChunkDirs = {
            Files.createTempDirectory("memmap").toFile().getAbsolutePath(),
            Files.createTempDirectory("memmap").toFile().getAbsolutePath()
        };
        ByteBufferProviderBackedMapChunkFactory byteBufferProviderBackedMapChunkFactory = new ByteBufferProviderBackedMapChunkFactory(
            4, false, 8, false, 100, new ByteBufferProvider("memmap", new HeapByteBufferFactory()));
        MiruHybridActivityIndex byteBufferHybridActivityIndex = new MiruHybridActivityIndex(
            new PartitionedMapChunkBackedKeyedStore(
                byteBufferProviderBackedMapChunkFactory,
                byteBufferProviderBackedMapChunkFactory,
                new ChunkStoreInitializer().initializeMultiFileBacked(byteBufferChunkDirs, "data", 4, 512, true),
                24),
            new MiruInternalActivityMarshaller());

        String[] fileBackedChunkDirs = {
            Files.createTempDirectory("memmap").toFile().getAbsolutePath(),
            Files.createTempDirectory("memmap").toFile().getAbsolutePath()
        };
        MiruHybridActivityIndex fileBackedHybridActivityIndex = new MiruHybridActivityIndex(
            new PartitionedMapChunkBackedKeyedStore(
                createFileBackedMapChunkFactory("memmap", 4, false, 8, false, 100, 2),
                createFileBackedMapChunkFactory("memmap", 4, false, 8, false, 100, 2),
                new ChunkStoreInitializer().initializeMultiFileBacked(fileBackedChunkDirs, "data", 4, 512, true),
                24),
            new MiruInternalActivityMarshaller());

        MiruTenantId tenantId = new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes());
        fileBackedHybridActivityIndex.bulkImport(tenantId, byteBufferHybridActivityIndex);

        return new Object[][] {
            { byteBufferHybridActivityIndex, false },
            { fileBackedHybridActivityIndex, false }, };
    }

    @DataProvider(name = "miruActivityIndexDataProviderWithData")
    public Object[][] miruActivityIndexDataProviderWithData() throws Exception {
        MiruTenantId tenantId = new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes());
        MiruInternalActivity miruActivity1 = buildMiruActivity(tenantId, 1, new String[0], 3);
        MiruInternalActivity miruActivity2 = buildMiruActivity(tenantId, 2, new String[0], 4);
        MiruInternalActivity miruActivity3 = buildMiruActivity(tenantId, 3, new String[] { "abcde" }, 1);
        final MiruInternalActivity[] miruActivities = new MiruInternalActivity[] { miruActivity1, miruActivity2, miruActivity3 };

        // Add activities to in-memory index
        HeapByteBufferFactory byteBufferFactory = new HeapByteBufferFactory();
        ByteBufferProviderBackedMapChunkFactory byteBufferFactoryBackedMapChunkFactory = new ByteBufferProviderBackedMapChunkFactory(
            4, false, 8, false, 100,
            new ByteBufferProvider("memmap", byteBufferFactory));
        MiruHybridActivityIndex byteBufferHybridActivityIndex = new MiruHybridActivityIndex(
            new PartitionedMapChunkBackedKeyedStore(
                byteBufferFactoryBackedMapChunkFactory,
                byteBufferFactoryBackedMapChunkFactory,
                new ChunkStoreInitializer().initializeMultiByteBufferBacked("chunks", byteBufferFactory, 4, 512, true),
                24),
            new MiruInternalActivityMarshaller());

        byteBufferHybridActivityIndex.bulkImport(tenantId, new BulkExport<Iterator<MiruInternalActivity>>() {
            @Override
            public Iterator<MiruInternalActivity> bulkExport(MiruTenantId tenantId) throws Exception {
                return Arrays.asList(miruActivities).iterator();
            }
        });

        // Add activities to mem-mapped index
        String[] fileBackedChunkDirs = {
            Files.createTempDirectory("memmap").toFile().getAbsolutePath(),
            Files.createTempDirectory("memmap").toFile().getAbsolutePath()
        };
        MiruHybridActivityIndex fileBackedHybridActivityIndex = new MiruHybridActivityIndex(
            new PartitionedMapChunkBackedKeyedStore(
                createFileBackedMapChunkFactory("memmap", 4, false, 8, false, 100, 2),
                createFileBackedMapChunkFactory("memmap", 4, false, 8, false, 100, 2),
                new ChunkStoreInitializer().initializeMultiFileBacked(fileBackedChunkDirs, "data", 4, 512, true),
                24),
            new MiruInternalActivityMarshaller());
        fileBackedHybridActivityIndex.bulkImport(tenantId, byteBufferHybridActivityIndex);

        return new Object[][] {
            { byteBufferHybridActivityIndex, miruActivities },
            { fileBackedHybridActivityIndex, miruActivities }
        };
    }

    private MiruHybridActivityIndex buildHybridIndex(MapChunkFactory mapChunkFactory, MapChunkFactory swapChunkFactory) throws Exception {
        // Add activities to first hybrid index
        String[] chunkDirs1 = {
            Files.createTempDirectory("memmap").toFile().getAbsolutePath(),
            Files.createTempDirectory("memmap").toFile().getAbsolutePath()
        };
        return new MiruHybridActivityIndex(
            new PartitionedMapChunkBackedKeyedStore(
                mapChunkFactory,
                swapChunkFactory,
                new ChunkStoreInitializer().initializeMultiFileBacked(chunkDirs1, "data", 4, 512, true),
                24),
            new MiruInternalActivityMarshaller());
    }

    private void testHybridToHybrid(MiruHybridActivityIndex hybridIndex1, MiruHybridActivityIndex hybridIndex2) throws Exception {
        MiruTenantId tenantId = new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes());
        MiruInternalActivity miruActivity1 = buildMiruActivity(tenantId, 1, new String[0], 3);
        MiruInternalActivity miruActivity2 = buildMiruActivity(tenantId, 2, new String[0], 4);
        MiruInternalActivity miruActivity3 = buildMiruActivity(tenantId, 3, new String[] { "abcde" }, 1);
        final MiruInternalActivity[] miruActivities = new MiruInternalActivity[] { miruActivity1, miruActivity2, miruActivity3 };

        hybridIndex1.bulkImport(tenantId, new BulkExport<Iterator<MiruInternalActivity>>() {
            @Override
            public Iterator<MiruInternalActivity> bulkExport(MiruTenantId tenantId) throws Exception {
                return Arrays.asList(miruActivities).iterator();
            }
        });

        hybridIndex2.bulkImport(tenantId, hybridIndex1);

        for (int i = 0; i < 3; i++) {
            assertEquals(hybridIndex1.get(tenantId, i), hybridIndex2.get(tenantId, i));
        }
    }

    @Test
    public void testHybridToHybrid_direct() throws Exception {
        ByteBufferProviderBackedMapChunkFactory mapChunkFactory = new ByteBufferProviderBackedMapChunkFactory(
            4, false, 8, false, 100,
            new ByteBufferProvider("test", new DirectByteBufferFactory()));
        testHybridToHybrid(
            buildHybridIndex(mapChunkFactory, mapChunkFactory),
            buildHybridIndex(mapChunkFactory, mapChunkFactory));
    }

    @Test
    public void testHybridToHybrid_memMap() throws Exception {
        testHybridToHybrid(
            buildHybridIndex(
                createFileBackedMapChunkFactory("memmap", 4, false, 8, false, 100, 2),
                createFileBackedMapChunkFactory("memmap", 4, false, 8, false, 100, 2)),
            buildHybridIndex(
                createFileBackedMapChunkFactory("memmap", 4, false, 8, false, 100, 2),
                createFileBackedMapChunkFactory("memmap", 4, false, 8, false, 100, 2)));
    }

    private MiruInternalActivity buildMiruActivity(MiruTenantId tenantId, long time, String[] authz, int numberOfRandomFields) {
        assertTrue(numberOfRandomFields <= schema.fieldCount());
        MiruInternalActivity.Builder builder = new MiruInternalActivity.Builder(schema, tenantId, time, authz, 0);

        for (int i = 0; i < numberOfRandomFields; i++) {
            builder.putFieldValue(schema.getFieldDefinition(i).name, RandomStringUtils.randomAlphanumeric(5));
        }
        return builder.build();
    }
}
