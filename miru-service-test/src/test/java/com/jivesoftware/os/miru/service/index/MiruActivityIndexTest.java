package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema;
import com.jivesoftware.os.miru.api.activity.schema.MiruSchema.Builder;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.bitmaps.roaring5.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.MiruInterner;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruActivityIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.plugin.index.MiruTermComposer;
import com.jivesoftware.os.miru.plugin.index.TimeVersionRealtime;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.RandomStringUtils;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.IndexTestUtil.buildInMemoryContext;
import static com.jivesoftware.os.miru.service.IndexTestUtil.buildOnDiskContext;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class MiruActivityIndexTest {

    MiruInterner<MiruTermId> termInterner = new MiruInterner<MiruTermId>(true) {
        @Override
        public MiruTermId create(byte[] bytes) {
            return new MiruTermId(bytes);
        }
    };

    MiruSchema schema = new MiruSchema.Builder("test", 1)
        .setFieldDefinitions(DefaultMiruSchemaDefinition.FIELDS)
        .build();
    MiruTermComposer termComposer = new MiruTermComposer(Charsets.UTF_8, termInterner);

    @Test
    public void testTermLookup() throws Exception {
        int numberOfFields = 3;
        int numberOfActivities = 1_000;

        StackBuffer stackBuffer = new StackBuffer();
        MiruTenantId tenantId = new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes());
        List<MiruActivityIndex> indexes = Arrays.asList(
            buildOnDiskActivityIndex(false, false), // filer
            buildOnDiskActivityIndex(true, false), // lab
            buildOnDiskActivityIndex(true, true)); // lab realtime
        for (MiruActivityIndex activityIndex : indexes) {
            List<MiruActivityAndId<MiruInternalActivity>> activityAndIds = Lists.newArrayList();
            for (int i = 0; i < numberOfActivities; i++) {
                activityAndIds.add(new MiruActivityAndId<>(buildLookupActivity(tenantId, i, new String[0], numberOfFields), i, i));
            }
            activityIndex.setAndReady(schema, activityAndIds, stackBuffer);

            for (int i = 0; i < numberOfActivities; i++) {
                MiruActivityAndId<MiruInternalActivity> activityAndId = activityAndIds.get(i);
                for (int j = 0; j < numberOfFields; j++) {
                    MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(j);
                    MiruTermId[] termIds = activityIndex.get("test", i, j, stackBuffer);
                    assertNotNull(termIds);
                    assertEquals(termIds.length, 1);
                    assertEquals(termIds[0], termComposer.compose(schema, fieldDefinition, stackBuffer, activityAndId.activity.time + "-" + j));
                }
            }
        }
    }

    private MiruInternalActivity buildLookupActivity(MiruTenantId tenantId, long time, String[] authz, int numberOfFields) throws Exception {
        assertTrue(numberOfFields <= schema.fieldCount());
        MiruInternalActivity.Builder builder = new MiruInternalActivity.Builder(schema, tenantId, time, 0, false, authz);
        StackBuffer stackBuffer = new StackBuffer();
        MiruTermId[][] terms = new MiruTermId[numberOfFields][];
        for (int i = 0; i < numberOfFields; i++) {
            MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(i);
            terms[i] = new MiruTermId[] { termComposer.compose(schema, fieldDefinition, stackBuffer, time + "-" + i) };
        }
        builder.putFieldsValues(terms);
        return builder.build();
    }

    @Test(dataProvider = "miruActivityIndexDataProvider")
    public void testSetActivity(MiruActivityIndex miruActivityIndex, boolean hasRealtime) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruSchema schema = new Builder("test", 1).build();
        MiruInternalActivity miruActivity = buildMiruActivity(new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes()), 1, true, new String[0], 5);
        try {
            miruActivityIndex.setAndReady(schema, Arrays.asList(new MiruActivityAndId<>(miruActivity, 0, 1L)), stackBuffer);
        } catch (UnsupportedOperationException e) {
            fail("This implementation of the MiruActivityIndex shouldn't have thrown an UnsupportedOperationException", e);
        }
    }

    @Test(dataProvider = "miruActivityIndexDataProvider")
    public void testSetActivityOutOfBounds(MiruActivityIndex miruActivityIndex, boolean hasRealtime) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruSchema schema = new Builder("test", 1).build();
        MiruInternalActivity miruActivity = buildMiruActivity(new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes()), 1, true, new String[0], 5);
        try {
            miruActivityIndex.setAndReady(schema, Arrays.asList(new MiruActivityAndId<>(miruActivity, -1, 1L)), stackBuffer);
            fail("Expected an IllegalArgumentException but never got it");
        } catch (UnsupportedOperationException e) {
            fail("This implementation of the MiruActivityIndex shouldn't have thrown an UnsupportedOperationException", e);
        } catch (IllegalArgumentException e) {
            // We want to get here, fall through
        }
    }

    @Test(dataProvider = "miruActivityIndexDataProvider")
    public void testTimeVersionRealtime(MiruActivityIndex miruActivityIndex, boolean hasRealtime) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruSchema schema = new Builder("test", 1).build();

        int count = 1_000;
        List<MiruActivityAndId<MiruInternalActivity>> activityAndIds = Lists.newArrayListWithCapacity(count);
        int[] ids = new int[count];
        for (int i = 0; i < count; i++) {
            MiruInternalActivity miruActivity = buildMiruActivity(new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes()),
                1000 + i, // timestamp
                i % 2 == 0, // realtimeDelivery
                new String[0],
                5);
            ids[i] = i;
            activityAndIds.add(new MiruActivityAndId<>(miruActivity, i, i));
        }

        miruActivityIndex.setAndReady(schema, activityAndIds, stackBuffer);

        TimeVersionRealtime[] tvrs = miruActivityIndex.getAllTimeVersionRealtime("test", ids, stackBuffer);
        assertNotNull(tvrs);
        assertEquals(tvrs.length, count);
        for (int i = 0; i < tvrs.length; i++) {
            assertNotNull(tvrs[i]);
            assertEquals(tvrs[i].timestamp, 1000 + i);
            assertEquals(tvrs[i].realtimeDelivery, hasRealtime && (i % 2 == 0));
        }
    }

    @Test(dataProvider = "miruActivityIndexDataProviderWithData")
    public void testGetActivity(MiruActivityIndex miruActivityIndex, MiruInternalActivity[] expectedActivities, boolean hasRealtime) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        assertTrue(expectedActivities.length == 3);
        assertEquals(miruActivityIndex.getTimeVersionRealtime("test", 0, stackBuffer).timestamp, expectedActivities[0].time);
        assertEquals(miruActivityIndex.getTimeVersionRealtime("test", 0, stackBuffer).version, expectedActivities[0].version);
        assertEquals(miruActivityIndex.getTimeVersionRealtime("test", 0, stackBuffer).realtimeDelivery, hasRealtime && expectedActivities[0].realtimeDelivery);
        assertEquals(miruActivityIndex.getTimeVersionRealtime("test", 1, stackBuffer).timestamp, expectedActivities[1].time);
        assertEquals(miruActivityIndex.getTimeVersionRealtime("test", 1, stackBuffer).version, expectedActivities[1].version);
        assertEquals(miruActivityIndex.getTimeVersionRealtime("test", 1, stackBuffer).realtimeDelivery, hasRealtime && expectedActivities[1].realtimeDelivery);
        assertEquals(miruActivityIndex.getTimeVersionRealtime("test", 2, stackBuffer).timestamp, expectedActivities[2].time);
        assertEquals(miruActivityIndex.getTimeVersionRealtime("test", 2, stackBuffer).version, expectedActivities[2].version);
        assertEquals(miruActivityIndex.getTimeVersionRealtime("test", 2, stackBuffer).realtimeDelivery, hasRealtime && expectedActivities[2].realtimeDelivery);
    }

    @Test(dataProvider = "miruActivityIndexDataProviderWithData", expectedExceptions = IllegalArgumentException.class)
    public void testGetActivityOverCapacity(MiruActivityIndex miruActivityIndex, MiruInternalActivity[] expectedActivities, boolean hasRealtime) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        miruActivityIndex.getTimeVersionRealtime("test", expectedActivities.length, stackBuffer); // This should throw an exception
    }

    @DataProvider(name = "miruActivityIndexDataProvider")
    public Object[][] miruActivityIndexDataProvider() throws Exception {
        MiruActivityIndex chunkInMemoryActivityIndex = buildInMemoryActivityIndex(false, false);
        MiruActivityIndex chunkOnDiskActivityIndex = buildOnDiskActivityIndex(false, false);
        MiruActivityIndex labInMemoryActivityIndex = buildInMemoryActivityIndex(true, false);
        MiruActivityIndex labInMemoryActivityIndexRealtime = buildInMemoryActivityIndex(true, true);
        MiruActivityIndex labOnDiskActivityIndex = buildOnDiskActivityIndex(true, false);
        MiruActivityIndex labOnDiskActivityIndexRealtime = buildOnDiskActivityIndex(true, true);

        return new Object[][] {
            { chunkInMemoryActivityIndex, false },
            { chunkOnDiskActivityIndex, false },
            { labInMemoryActivityIndex, false },
            { labInMemoryActivityIndexRealtime, true },
            { labOnDiskActivityIndex, false },
            { labOnDiskActivityIndexRealtime, true },
        };
    }

    @DataProvider(name = "miruActivityIndexDataProviderWithData")
    public Object[][] miruActivityIndexDataProviderWithData() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruSchema schema = new Builder("test", 1).build();
        MiruTenantId tenantId = new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes());
        MiruInternalActivity miruActivity1 = buildMiruActivity(tenantId, 1, true, new String[0], 3);
        MiruInternalActivity miruActivity2 = buildMiruActivity(tenantId, 2, false, new String[0], 4);
        MiruInternalActivity miruActivity3 = buildMiruActivity(tenantId, 3, true, new String[] { "abcde" }, 1);
        final MiruInternalActivity[] miruActivities = new MiruInternalActivity[] { miruActivity1, miruActivity2, miruActivity3 };

        List<MiruActivityAndId<MiruInternalActivity>> activityAndIds = Arrays.asList(
            new MiruActivityAndId<>(miruActivity1, 0, 1L),
            new MiruActivityAndId<>(miruActivity2, 1, 2L),
            new MiruActivityAndId<>(miruActivity3, 2, 3L));

        // Add activities to in-memory index
        MiruActivityIndex chunkInMemoryActivityIndex = buildInMemoryActivityIndex(false, false);
        MiruActivityIndex labInMemoryActivityIndex = buildInMemoryActivityIndex(true, false);
        MiruActivityIndex labInMemoryActivityIndexRealtime = buildInMemoryActivityIndex(true, true);
        chunkInMemoryActivityIndex.setAndReady(schema, activityAndIds, stackBuffer);
        labInMemoryActivityIndex.setAndReady(schema, activityAndIds, stackBuffer);
        labInMemoryActivityIndexRealtime.setAndReady(schema, activityAndIds, stackBuffer);

        MiruActivityIndex chunkOnDiskActivityIndex = buildOnDiskActivityIndex(false, false);
        MiruActivityIndex labOnDiskActivityIndex = buildOnDiskActivityIndex(true, false);
        MiruActivityIndex labOnDiskActivityIndexRealtime = buildOnDiskActivityIndex(true, true);
        chunkOnDiskActivityIndex.setAndReady(schema, activityAndIds, stackBuffer);
        labOnDiskActivityIndex.setAndReady(schema, activityAndIds, stackBuffer);
        labOnDiskActivityIndexRealtime.setAndReady(schema, activityAndIds, stackBuffer);

        return new Object[][] {
            { chunkInMemoryActivityIndex, miruActivities, false },
            { chunkOnDiskActivityIndex, miruActivities, false },
            { labInMemoryActivityIndex, miruActivities, false },
            { labOnDiskActivityIndex, miruActivities, false },
            { labInMemoryActivityIndexRealtime, miruActivities, true },
            { labOnDiskActivityIndexRealtime, miruActivities, true },
        };
    }

    private MiruActivityIndex buildInMemoryActivityIndex(boolean useLabIndexes, boolean hasRealtime) throws Exception {
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("logicalName"));
        MiruContext<RoaringBitmap, RoaringBitmap, ?> hybridContext = buildInMemoryContext(4, useLabIndexes, hasRealtime, bitmaps, coord);
        return hybridContext.activityIndex;
    }

    private MiruActivityIndex buildOnDiskActivityIndex(boolean useLabIndexes, boolean hasRealtime) throws Exception {
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("logicalName"));
        MiruContext<RoaringBitmap, RoaringBitmap, ?> hybridContext = buildOnDiskContext(4, useLabIndexes, hasRealtime, bitmaps, coord);
        return hybridContext.activityIndex;
    }

    private MiruInternalActivity buildMiruActivity(MiruTenantId tenantId,
        long time,
        boolean realtimeDelivery,
        String[] authz,
        int numberOfRandomFields) throws Exception {
        assertTrue(numberOfRandomFields <= schema.fieldCount());
        MiruInternalActivity.Builder builder = new MiruInternalActivity.Builder(schema, tenantId, time, 0, realtimeDelivery, authz);
        StackBuffer stackBuffer = new StackBuffer();
        MiruTermId[][] terms = new MiruTermId[numberOfRandomFields][];
        for (int i = 0; i < numberOfRandomFields; i++) {
            MiruFieldDefinition fieldDefinition = schema.getFieldDefinition(i);
            terms[i] = new MiruTermId[] { termComposer.compose(schema, fieldDefinition, stackBuffer, RandomStringUtils.randomAlphanumeric(5)) };
        }
        builder.putFieldsValues(terms);
        return builder.build();
    }
}
