package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.chunk.store.ChunkStoreInitializer;
import com.jivesoftware.os.filer.io.Filer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.service.index.memory.MiruHybridActivityIndex;
import com.jivesoftware.os.miru.service.index.memory.MiruInMemoryActivityIndex;
import com.jivesoftware.os.miru.service.locator.DiskIdentifierPartResourceLocator;
import com.jivesoftware.os.miru.service.locator.MiruPartitionCoordIdentifier;
import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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

    @Test (dataProvider = "miruActivityIndexDataProvider")
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

    @Test (dataProvider = "miruActivityIndexDataProvider")
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

    @Test (dataProvider = "miruActivityIndexDataProviderWithData")
    public void testGetActivity(MiruActivityIndex miruActivityIndex, MiruInternalActivity[] expectedActivities) {
        assertTrue(expectedActivities.length == 3);
        assertEquals(miruActivityIndex.get(expectedActivities[0].tenantId, 0), expectedActivities[0]);
        assertEquals(miruActivityIndex.get(expectedActivities[1].tenantId, 1), expectedActivities[1]);
        assertEquals(miruActivityIndex.get(expectedActivities[2].tenantId, 2), expectedActivities[2]);
    }

    @Test (dataProvider = "miruActivityIndexDataProviderWithData", expectedExceptions = IllegalArgumentException.class)
    public void testGetActivityOverCapacity(MiruActivityIndex miruActivityIndex, MiruInternalActivity[] expectedActivities) {
        miruActivityIndex.get(null, expectedActivities.length); // This should throw an exception
    }

    @DataProvider (name = "miruActivityIndexDataProvider")
    public Object[][] miruActivityIndexDataProvider() throws Exception {
        MiruInMemoryActivityIndex miruInMemoryActivityIndex = new MiruInMemoryActivityIndex();

        MiruTenantId tenantId = new MiruTenantId(new byte[]{ 1 });

        String[] mapDirs = {
            Files.createTempDirectory("memmap").toFile().getAbsolutePath(),
            Files.createTempDirectory("memmap").toFile().getAbsolutePath()
        };
        String[] swapDirs = {
            Files.createTempDirectory("memmap").toFile().getAbsolutePath(),
            Files.createTempDirectory("memmap").toFile().getAbsolutePath()
        };
        String[] chunkDirs = {
            Files.createTempDirectory("memmap").toFile().getAbsolutePath(),
            Files.createTempDirectory("memmap").toFile().getAbsolutePath()
        };
        MiruHybridActivityIndex hybridActivityIndex = new MiruHybridActivityIndex(
            mapDirs,
            swapDirs,
            new ChunkStoreInitializer().initializeMulti(chunkDirs, "data", 4, 512, true),
            new MiruInternalActivityMarshaller(),
            Optional.<Filer>absent());

        hybridActivityIndex.bulkImport(tenantId, miruInMemoryActivityIndex);

        return new Object[][]{
            { miruInMemoryActivityIndex, false },
            { hybridActivityIndex, false }, };
    }

    @DataProvider (name = "miruActivityIndexDataProviderWithData")
    public Object[][] miruActivityIndexDataProviderWithData() throws Exception {
        MiruTenantId tenantId = new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes());
        MiruInternalActivity miruActivity1 = buildMiruActivity(tenantId, 1, new String[0], 3);
        MiruInternalActivity miruActivity2 = buildMiruActivity(tenantId, 2, new String[0], 4);
        MiruInternalActivity miruActivity3 = buildMiruActivity(tenantId, 3, new String[]{ "abcde" }, 1);
        final MiruInternalActivity[] miruActivities = new MiruInternalActivity[]{ miruActivity1, miruActivity2, miruActivity3 };

        // Add activities to in-memory index
        MiruInMemoryActivityIndex miruInMemoryActivityIndex = new MiruInMemoryActivityIndex();
        miruInMemoryActivityIndex.bulkImport(tenantId, new BulkExport<Iterator<MiruInternalActivity>>() {
            @Override
            public Iterator<MiruInternalActivity> bulkExport(MiruTenantId tenantId) throws Exception {
                return Arrays.asList(miruActivities).iterator();
            }
        });

        // Add activities to mem-mapped index
        String[] mapDirs = {
            Files.createTempDirectory("memmap").toFile().getAbsolutePath(),
            Files.createTempDirectory("memmap").toFile().getAbsolutePath()
        };
        String[] swapDirs = {
            Files.createTempDirectory("memmap").toFile().getAbsolutePath(),
            Files.createTempDirectory("memmap").toFile().getAbsolutePath()
        };
        String[] chunkDirs = {
            Files.createTempDirectory("memmap").toFile().getAbsolutePath(),
            Files.createTempDirectory("memmap").toFile().getAbsolutePath()
        };
        MiruHybridActivityIndex miruMemMappedActivityIndex = new MiruHybridActivityIndex(
            mapDirs,
            swapDirs,
            new ChunkStoreInitializer().initializeMulti(chunkDirs, "data", 4, 512, true),
            new MiruInternalActivityMarshaller(),
            Optional.<Filer>absent());
        miruMemMappedActivityIndex.bulkImport(tenantId, miruInMemoryActivityIndex);

        return new Object[][]{
            { miruInMemoryActivityIndex, miruActivities },
            { miruMemMappedActivityIndex, miruActivities }
        };
    }

    private void testHybridToHybrid(Optional<Filer> filer1, Optional<Filer> filer2) throws Exception {
        MiruTenantId tenantId = new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes());
        MiruInternalActivity miruActivity1 = buildMiruActivity(tenantId, 1, new String[0], 3);
        MiruInternalActivity miruActivity2 = buildMiruActivity(tenantId, 2, new String[0], 4);
        MiruInternalActivity miruActivity3 = buildMiruActivity(tenantId, 3, new String[]{ "abcde" }, 1);
        final MiruInternalActivity[] miruActivities = new MiruInternalActivity[]{ miruActivity1, miruActivity2, miruActivity3 };

        // Add activities to first hybrid index
        String[] mapDirs1 = {
            Files.createTempDirectory("memmap").toFile().getAbsolutePath(),
            Files.createTempDirectory("memmap").toFile().getAbsolutePath()
        };
        String[] swapDirs1 = {
            Files.createTempDirectory("memmap").toFile().getAbsolutePath(),
            Files.createTempDirectory("memmap").toFile().getAbsolutePath()
        };
        String[] chunkDirs1 = {
            Files.createTempDirectory("memmap").toFile().getAbsolutePath(),
            Files.createTempDirectory("memmap").toFile().getAbsolutePath()
        };
        MiruHybridActivityIndex hybridIndex1 = new MiruHybridActivityIndex(
            mapDirs1,
            swapDirs1,
            new ChunkStoreInitializer().initializeMulti(chunkDirs1, "data", 4, 512, true),
            new MiruInternalActivityMarshaller(),
            filer1);
        hybridIndex1.bulkImport(tenantId, new BulkExport<Iterator<MiruInternalActivity>>() {
            @Override
            public Iterator<MiruInternalActivity> bulkExport(MiruTenantId tenantId) throws Exception {
                return Arrays.asList(miruActivities).iterator();
            }
        });

        // Copy activities to second hybrid index
        String[] mapDirs2 = {
            Files.createTempDirectory("memmap").toFile().getAbsolutePath(),
            Files.createTempDirectory("memmap").toFile().getAbsolutePath()
        };
        String[] swapDirs2 = {
            Files.createTempDirectory("memmap").toFile().getAbsolutePath(),
            Files.createTempDirectory("memmap").toFile().getAbsolutePath()
        };
        String[] chunkDirs2 = {
            Files.createTempDirectory("memmap").toFile().getAbsolutePath(),
            Files.createTempDirectory("memmap").toFile().getAbsolutePath()
        };
        MiruHybridActivityIndex hybridIndex2 = new MiruHybridActivityIndex(
            mapDirs2,
            swapDirs2,
            new ChunkStoreInitializer().initializeMulti(chunkDirs2, "data", 4, 512, true),
            new MiruInternalActivityMarshaller(),
            filer2);
        hybridIndex2.bulkImport(tenantId, hybridIndex1);

        for (int i = 0; i < 3; i++) {
            assertEquals(hybridIndex1.get(tenantId, i), hybridIndex2.get(tenantId, i));
        }
    }

    @Test
    public void testHybridToHybrid_noFiler() throws Exception {
        testHybridToHybrid(Optional.<Filer>absent(), Optional.<Filer>absent());
    }

    @Test
    public void testHybridToHybrid_randomAccessFiler() throws Exception {
        MiruTenantId tenantId = new MiruTenantId("tenant1".getBytes());
        MiruPartitionId partitionId = MiruPartitionId.of(0);
        MiruPartitionCoord coord1 = new MiruPartitionCoord(tenantId, partitionId, new MiruHost("localhost", 49601));
        MiruPartitionCoord coord2 = new MiruPartitionCoord(tenantId, partitionId, new MiruHost("localhost", 49602));
        File[] dirs = { Files.createTempDirectory("hybridToHybrid").toFile() };
        DiskIdentifierPartResourceLocator resourceLocator = new DiskIdentifierPartResourceLocator(dirs, 4);
        testHybridToHybrid(
            Optional.<Filer>of(resourceLocator.getRandomAccessFiler(new MiruPartitionCoordIdentifier(coord1), "activity", "rw")),
            Optional.<Filer>of(resourceLocator.getRandomAccessFiler(new MiruPartitionCoordIdentifier(coord2), "activity", "rw")));
    }

    @Test
    public void testHybridToHybrid_byteBufferFiler() throws Exception {
        MiruTenantId tenantId = new MiruTenantId("tenant1".getBytes());
        MiruPartitionId partitionId = MiruPartitionId.of(0);
        MiruPartitionCoord coord1 = new MiruPartitionCoord(tenantId, partitionId, new MiruHost("localhost", 49601));
        MiruPartitionCoord coord2 = new MiruPartitionCoord(tenantId, partitionId, new MiruHost("localhost", 49602));
        File[] dirs = { Files.createTempDirectory("hybridToHybrid").toFile() };
        DiskIdentifierPartResourceLocator resourceLocator = new DiskIdentifierPartResourceLocator(dirs, 4);
        testHybridToHybrid(
            Optional.<Filer>of(resourceLocator.getByteBufferBackedFiler(new MiruPartitionCoordIdentifier(coord1), "activity", 4)),
            Optional.<Filer>of(resourceLocator.getByteBufferBackedFiler(new MiruPartitionCoordIdentifier(coord2), "activity", 4)));
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
