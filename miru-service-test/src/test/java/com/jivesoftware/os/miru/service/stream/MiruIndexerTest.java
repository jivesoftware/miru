package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.MoreExecutors;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.bitmaps.roaring5.buffer.MiruBitmapsRoaringBuffer;
import com.jivesoftware.os.miru.plugin.index.BloomIndex;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.IndexTestUtil;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MiruIndexerTest {

    @Test(dataProvider = "miruIndexContextDataProvider")
    public void testIndexData(MiruPartitionCoord coord,
        MiruContext<MutableRoaringBitmap, ?> context,
        MiruIndexer<MutableRoaringBitmap> miruIndexer,
        List<MiruActivityAndId<MiruActivity>> activityList)
        throws Exception {

        byte[] primitiveBuffer = new byte[8];
        MiruTenantId tenantId = coord.tenantId;

        // First check existing data
        verifyFieldValues(tenantId, context, 0, 0, primitiveBuffer);
        verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, 0, primitiveBuffer).authz, 0, primitiveBuffer);
        verifyFieldValues(tenantId, context, 1, 0, primitiveBuffer);
        verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, 1, primitiveBuffer).authz, 1, primitiveBuffer);
        verifyFieldValues(tenantId, context, 2, 0, primitiveBuffer);
        verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, 2, primitiveBuffer).authz, 2, primitiveBuffer);

        // Next add new data and check it
        miruIndexer.index(
            context,
            coord,
            Lists.newArrayList(Arrays.asList(new MiruActivityAndId<>(
                buildMiruActivity(tenantId, 4, new String[] { "pqrst" }, ImmutableMap.of(
                    DefaultMiruSchemaDefinition.FIELDS[0].name, "0",
                    DefaultMiruSchemaDefinition.FIELDS[1].name, "1")),
                3))),
            false,
            MoreExecutors.sameThreadExecutor());
        verifyFieldValues(tenantId, context, 3, 0, primitiveBuffer);
        verifyFieldValues(tenantId, context, 3, 1, primitiveBuffer);
        verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, 3, primitiveBuffer).authz, 3, primitiveBuffer);

        miruIndexer.index(
            context,
            coord,
            Lists.newArrayList(Arrays.asList(new MiruActivityAndId<>(
                buildMiruActivity(tenantId, 5, new String[] { "uvwxy" }, ImmutableMap.of(
                    DefaultMiruSchemaDefinition.FIELDS[0].name, "0",
                    DefaultMiruSchemaDefinition.FIELDS[2].name, "2")),
                4))),
            false,
            MoreExecutors.sameThreadExecutor());
        verifyFieldValues(tenantId, context, 4, 0, primitiveBuffer);
        verifyFieldValues(tenantId, context, 4, 2, primitiveBuffer);
        verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, 4, primitiveBuffer).authz, 4, primitiveBuffer);
    }

    @Test(dataProvider = "miruIndexContextDataProvider")
    public void testRepairData(MiruPartitionCoord coord,
        MiruContext<MutableRoaringBitmap, ?> context,
        MiruIndexer<MutableRoaringBitmap> miruIndexer,
        List<MiruActivityAndId<MiruActivity>> activityList)
        throws Exception {

        byte[] primitiveBuffer = new byte[8];
        MiruTenantId tenantId = coord.tenantId;

        List<MiruActivityAndId<MiruActivity>> activityAndIds = Lists.newArrayList();
        for (MiruActivityAndId<MiruActivity> activityAndId : activityList) {
            MiruActivity activity = activityAndId.activity;
            int id = activityAndId.id;

            String[] authz = new String[activity.authz.length + 1];
            System.arraycopy(activity.authz, 0, authz, 0, activity.authz.length);
            authz[authz.length - 1] = "pqrst";

            activityAndIds.add(new MiruActivityAndId<>(
                buildMiruActivity(tenantId,
                    activity.time,
                    authz,
                    ImmutableMap.<String, String>builder()
                        .put(DefaultMiruSchemaDefinition.FIELDS[0].name, "0")
                        .put(DefaultMiruSchemaDefinition.FIELDS[1].name, "1")
                        .build()),
                id));
        }

        int nextId = activityList.size();
        activityAndIds.add(new MiruActivityAndId<>(
            buildMiruActivity(tenantId,
                nextId + 1,
                new String[] { "pqrst" },
                ImmutableMap.of(
                    DefaultMiruSchemaDefinition.FIELDS[0].name, "0",
                    DefaultMiruSchemaDefinition.FIELDS[1].name, "1")),
            nextId));

        // Repair data
        miruIndexer.index(context, coord, activityAndIds, true, MoreExecutors.sameThreadExecutor());

        // First check existing data
        for (MiruActivityAndId<MiruActivity> activityAndId : activityList) {
            verifyFieldValues(tenantId, context, activityAndId.id, 0, primitiveBuffer);
            verifyFieldValues(tenantId, context, activityAndId.id, 1, primitiveBuffer);
            verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, activityAndId.id, primitiveBuffer).authz, activityAndId.id,
                primitiveBuffer);
        }

        // And check new data
        verifyFieldValues(tenantId, context, nextId, 0, primitiveBuffer);
        verifyFieldValues(tenantId, context, nextId, 1, primitiveBuffer);
        verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, nextId, primitiveBuffer).authz, nextId, primitiveBuffer);
    }

    private void verifyFieldValues(MiruTenantId tenantId, MiruContext<MutableRoaringBitmap, ?> context, int activityId, int fieldId, byte[] primitiveBuffer)
        throws Exception {

        MiruInternalActivity miruActivity = context.getActivityIndex().get(tenantId, activityId, primitiveBuffer);

        MiruTermId[] fieldValues = miruActivity.fieldsValues[fieldId];
        if (fieldValues == null) {
            fieldValues = new MiruTermId[0];
        }
        for (MiruTermId fieldValue : fieldValues) {
            MiruInvertedIndex<MutableRoaringBitmap> invertedIndex = context.getFieldIndexProvider()
                .getFieldIndex(MiruFieldType.primary)
                .get(fieldId, fieldValue);
            assertNotNull(invertedIndex);
            MutableRoaringBitmap bitmap = invertedIndex.getIndex(primitiveBuffer).get();
            assertNotNull(bitmap);
            assertTrue(bitmap.contains(activityId));
        }
    }

    private void verifyAuthzValues(MiruAuthzIndex<MutableRoaringBitmap> miruAuthzIndex,
        String[] authzs,
        int activityId,
        byte[] primitiveBuffer) throws Exception {
        MiruAuthzExpression miruAuthzExpression = new MiruAuthzExpression(Arrays.asList(authzs));

        MutableRoaringBitmap compositeAuthz = miruAuthzIndex.getCompositeAuthz(miruAuthzExpression, primitiveBuffer);
        assertTrue(compositeAuthz.contains(activityId));
    }

    @DataProvider(name = "miruIndexContextDataProvider")
    public Object[][] miruIndexContextDataProvider() throws Exception {
        final MiruTenantId tenantId = new MiruTenantId("indexContextTenant".getBytes());
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("localhost", 10000));

        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
        MiruIndexer<MutableRoaringBitmap> miruIndexer = new MiruIndexer<>(new MiruIndexAuthz<>(),
            new MiruIndexFieldValues<>(),
            new MiruIndexBloom<>(new BloomIndex<>(bitmaps, Hashing.murmur3_128(), 100_000, 0.01f)),
            new MiruIndexLatest<>(),
            new MiruIndexPairedLatest<>());

        MiruContext<MutableRoaringBitmap, ?> inMemoryContext = IndexTestUtil.buildInMemoryContext(4, bitmaps, coord);

        // Build in-memory index stream object
        MiruActivity miruActivity1 = buildMiruActivity(tenantId, 1, new String[] { "abcde" },
            ImmutableMap.of(DefaultMiruSchemaDefinition.FIELDS[0].name, "0"));
        MiruActivity miruActivity2 = buildMiruActivity(tenantId, 2, new String[] { "abcde" },
            ImmutableMap.of(DefaultMiruSchemaDefinition.FIELDS[0].name, "0"));
        MiruActivity miruActivity3 = buildMiruActivity(tenantId, 3, new String[] { "abcde" },
            ImmutableMap.of(DefaultMiruSchemaDefinition.FIELDS[0].name, "0"));
        List<MiruActivityAndId<MiruActivity>> immutableActivityList = Arrays.asList(
            new MiruActivityAndId<>(miruActivity1, 0),
            new MiruActivityAndId<>(miruActivity2, 1),
            new MiruActivityAndId<>(miruActivity3, 2));

        MiruContext<MutableRoaringBitmap, ?> onDiskContext = IndexTestUtil.buildOnDiskContext(4, bitmaps, coord);

        // Index initial activities
        miruIndexer.index(inMemoryContext,
            coord,
            Lists.newArrayList(immutableActivityList),
            false,
            MoreExecutors.sameThreadExecutor());

        miruIndexer.index(onDiskContext,
            coord,
            Lists.newArrayList(immutableActivityList),
            false,
            MoreExecutors.sameThreadExecutor());

        return new Object[][] {
            { coord, inMemoryContext, miruIndexer, immutableActivityList },
            { coord, onDiskContext, miruIndexer, immutableActivityList }
        };
    }

    private MiruActivity buildMiruActivity(MiruTenantId tenantId, long time, String[] authz, Map<String, String> fields) {
        MiruActivity.Builder builder = new MiruActivity.Builder(tenantId, time, authz, 0);
        for (Map.Entry<String, String> field : fields.entrySet()) {
            builder.putFieldValue(field.getKey(), field.getValue());
        }
        return builder.build();
    }
}
