package com.jivesoftware.os.miru.service.index;

import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import com.jivesoftware.os.miru.service.stream.allocator.MiruContextAllocator;
import java.util.Arrays;
import org.apache.commons.lang.RandomStringUtils;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.IndexTestUtil.buildHybridContextAllocator;
import static com.jivesoftware.os.miru.service.IndexTestUtil.buildOnDiskContextAllocator;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class MiruActivityIndexTest {

    MiruSchema schema = new MiruSchema(DefaultMiruSchemaDefinition.FIELDS);

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
        MiruActivityIndex hybridActivityIndex = buildHybridActivityIndex();
        MiruActivityIndex onDiskActivityIndex = buildOnDiskActivityIndex();

        MiruTenantId tenantId = new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes());
        ((BulkImport) onDiskActivityIndex).bulkImport(tenantId, (BulkExport) hybridActivityIndex);

        return new Object[][] {
            { hybridActivityIndex, false },
            { onDiskActivityIndex, false }, };
    }

    @DataProvider(name = "miruActivityIndexDataProviderWithData")
    public Object[][] miruActivityIndexDataProviderWithData() throws Exception {
        MiruTenantId tenantId = new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes());
        MiruInternalActivity miruActivity1 = buildMiruActivity(tenantId, 1, new String[0], 3);
        MiruInternalActivity miruActivity2 = buildMiruActivity(tenantId, 2, new String[0], 4);
        MiruInternalActivity miruActivity3 = buildMiruActivity(tenantId, 3, new String[] { "abcde" }, 1);
        final MiruInternalActivity[] miruActivities = new MiruInternalActivity[] { miruActivity1, miruActivity2, miruActivity3 };

        // Add activities to in-memory index
        MiruActivityIndex hybridActivityIndex = buildHybridActivityIndex();
        hybridActivityIndex.setAndReady(Arrays.asList(
            new MiruActivityAndId<>(miruActivity1, 0),
            new MiruActivityAndId<>(miruActivity2, 1),
            new MiruActivityAndId<>(miruActivity3, 2)));

        MiruActivityIndex onDiskActivityIndex = buildOnDiskActivityIndex();

        // Add activities to mem-mapped index
        ((BulkImport) onDiskActivityIndex).bulkImport(tenantId, (BulkExport) hybridActivityIndex);

        return new Object[][] {
            { hybridActivityIndex, miruActivities },
            { onDiskActivityIndex, miruActivities }
        };
    }

    private MiruActivityIndex buildHybridActivityIndex() throws Exception {
        MiruContextAllocator allocator = buildHybridContextAllocator(4, 10, true);
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("localhost", 10000));
        MiruContext<RoaringBitmap> hybridContext = allocator.allocate(bitmaps, coord);
        return hybridContext.activityIndex;
    }

    private MiruActivityIndex buildOnDiskActivityIndex() throws Exception {
        MiruContextAllocator allocator = buildOnDiskContextAllocator(4, 10);
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("localhost", 10000));
        MiruContext<RoaringBitmap> hybridContext = allocator.allocate(bitmaps, coord);
        return hybridContext.activityIndex;
    }

    @Test
    public void testIndexToIndex_hybrid_hybrid() throws Exception {
        testIndexToIndex(
            buildHybridActivityIndex(),
            buildHybridActivityIndex());
    }

    @Test
    public void testIndexToIndex_hybrid_disk() throws Exception {
        testIndexToIndex(
            buildHybridActivityIndex(),
            buildOnDiskActivityIndex());
    }

    @Test
    public void testIndexToIndex_disk_disk() throws Exception {
        testIndexToIndex(
            buildOnDiskActivityIndex(),
            buildOnDiskActivityIndex());
    }

    private void testIndexToIndex(MiruActivityIndex index1, MiruActivityIndex index2) throws Exception {
        MiruTenantId tenantId = new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes());
        MiruInternalActivity miruActivity1 = buildMiruActivity(tenantId, 1, new String[0], 3);
        MiruInternalActivity miruActivity2 = buildMiruActivity(tenantId, 2, new String[0], 4);
        MiruInternalActivity miruActivity3 = buildMiruActivity(tenantId, 3, new String[] { "abcde" }, 1);

        // Add activities to first hybrid index
        index1.setAndReady(Arrays.asList(
            new MiruActivityAndId<>(miruActivity1, 0),
            new MiruActivityAndId<>(miruActivity2, 1),
            new MiruActivityAndId<>(miruActivity3, 2)));

        ((BulkImport) index2).bulkImport(tenantId, (BulkExport) index1);

        for (int i = 0; i < 3; i++) {
            assertEquals(index1.get(tenantId, i), index2.get(tenantId, i));
        }
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
