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
import com.jivesoftware.os.miru.service.stream.MiruContext;
import java.io.IOException;
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
        MiruActivityIndex activityIndex = buildOnDiskActivityIndex();
        List<MiruActivityAndId<MiruInternalActivity>> activityAndIds = Lists.newArrayList();
        for (int i = 0; i < numberOfActivities; i++) {
            activityAndIds.add(new MiruActivityAndId<>(buildLookupActivity(tenantId, i, new String[0], numberOfFields), i));
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

    private MiruInternalActivity buildLookupActivity(MiruTenantId tenantId, long time, String[] authz, int numberOfFields) throws Exception {
        assertTrue(numberOfFields <= schema.fieldCount());
        MiruInternalActivity.Builder builder = new MiruInternalActivity.Builder(schema, tenantId, time, authz, 0);
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
    public void testSetActivity(MiruActivityIndex miruActivityIndex, boolean throwsUnsupportedExceptionOnSet) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruSchema schema = new Builder("test", 1).build();
        MiruInternalActivity miruActivity = buildMiruActivity(new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes()), 1, new String[0], 5);
        try {
            miruActivityIndex.setAndReady(schema, Arrays.asList(new MiruActivityAndId<>(miruActivity, 0)), stackBuffer);
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
        StackBuffer stackBuffer = new StackBuffer();
        MiruSchema schema = new Builder("test", 1).build();
        MiruInternalActivity miruActivity = buildMiruActivity(new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes()), 1, new String[0], 5);
        try {
            miruActivityIndex.setAndReady(schema, Arrays.asList(new MiruActivityAndId<>(miruActivity, -1)), stackBuffer);
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
    public void testGetActivity(MiruActivityIndex miruActivityIndex, MiruInternalActivity[] expectedActivities) throws IOException, InterruptedException {
        StackBuffer stackBuffer = new StackBuffer();
        assertTrue(expectedActivities.length == 3);
        assertEquals(miruActivityIndex.get("test", expectedActivities[0].tenantId, 0, stackBuffer), expectedActivities[0]);
        assertEquals(miruActivityIndex.get("test", expectedActivities[1].tenantId, 1, stackBuffer), expectedActivities[1]);
        assertEquals(miruActivityIndex.get("test", expectedActivities[2].tenantId, 2, stackBuffer), expectedActivities[2]);
    }

    @Test(dataProvider = "miruActivityIndexDataProviderWithData", expectedExceptions = IllegalArgumentException.class)
    public void testGetActivityOverCapacity(MiruActivityIndex miruActivityIndex, MiruInternalActivity[] expectedActivities) throws IOException,
        InterruptedException {
        StackBuffer stackBuffer = new StackBuffer();
        miruActivityIndex.get("test", null, expectedActivities.length, stackBuffer); // This should throw an exception
    }

    @DataProvider(name = "miruActivityIndexDataProvider")
    public Object[][] miruActivityIndexDataProvider() throws Exception {
        MiruActivityIndex hybridActivityIndex = buildInMemoryActivityIndex();
        MiruActivityIndex onDiskActivityIndex = buildOnDiskActivityIndex();

        return new Object[][] {
            { hybridActivityIndex, false },
            { onDiskActivityIndex, false } };
    }

    @DataProvider(name = "miruActivityIndexDataProviderWithData")
    public Object[][] miruActivityIndexDataProviderWithData() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruSchema schema = new Builder("test", 1).build();
        MiruTenantId tenantId = new MiruTenantId(RandomStringUtils.randomAlphabetic(10).getBytes());
        MiruInternalActivity miruActivity1 = buildMiruActivity(tenantId, 1, new String[0], 3);
        MiruInternalActivity miruActivity2 = buildMiruActivity(tenantId, 2, new String[0], 4);
        MiruInternalActivity miruActivity3 = buildMiruActivity(tenantId, 3, new String[] { "abcde" }, 1);
        final MiruInternalActivity[] miruActivities = new MiruInternalActivity[] { miruActivity1, miruActivity2, miruActivity3 };

        // Add activities to in-memory index
        MiruActivityIndex hybridActivityIndex = buildInMemoryActivityIndex();
        hybridActivityIndex.setAndReady(schema, Arrays.asList(
            new MiruActivityAndId<>(miruActivity1, 0),
            new MiruActivityAndId<>(miruActivity2, 1),
            new MiruActivityAndId<>(miruActivity3, 2)), stackBuffer);

        MiruActivityIndex onDiskActivityIndex = buildOnDiskActivityIndex();
        onDiskActivityIndex.setAndReady(schema, Arrays.asList(
            new MiruActivityAndId<>(miruActivity1, 0),
            new MiruActivityAndId<>(miruActivity2, 1),
            new MiruActivityAndId<>(miruActivity3, 2)), stackBuffer);

        return new Object[][] {
            { hybridActivityIndex, miruActivities },
            { onDiskActivityIndex, miruActivities }
        };
    }

    private MiruActivityIndex buildInMemoryActivityIndex() throws Exception {
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("logicalName"));
        MiruContext<RoaringBitmap, RoaringBitmap, ?> hybridContext = buildInMemoryContext(4, bitmaps, coord);
        return hybridContext.activityIndex;
    }

    private MiruActivityIndex buildOnDiskActivityIndex() throws Exception {
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruPartitionCoord coord = new MiruPartitionCoord(new MiruTenantId("test".getBytes()), MiruPartitionId.of(0), new MiruHost("logicalName"));
        MiruContext<RoaringBitmap, RoaringBitmap, ?> hybridContext = buildOnDiskContext(4, bitmaps, coord);
        return hybridContext.activityIndex;
    }

    private MiruInternalActivity buildMiruActivity(MiruTenantId tenantId, long time, String[] authz, int numberOfRandomFields) throws Exception {
        assertTrue(numberOfRandomFields <= schema.fieldCount());
        MiruInternalActivity.Builder builder = new MiruInternalActivity.Builder(schema, tenantId, time, authz, 0);
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
