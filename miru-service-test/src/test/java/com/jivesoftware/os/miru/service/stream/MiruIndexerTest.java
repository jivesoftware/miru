package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.MoreExecutors;
import com.jivesoftware.os.filer.io.api.StackBuffer;
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
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MiruIndexerTest {

    @Test(dataProvider = "miruIndexContextDataProvider")
    public void testIndexData(MiruPartitionCoord coord,
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, ?> context,
        MiruIndexer<MutableRoaringBitmap, ImmutableRoaringBitmap> miruIndexer,
        List<MiruActivityAndId<MiruActivity>> activityList)
        throws Exception {

        StackBuffer stackBuffer = new StackBuffer();
        MiruTenantId tenantId = coord.tenantId;

        // First check existing data
        verifyFieldValues(tenantId, context, 0, 0, stackBuffer);
        verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, 0, stackBuffer).authz, 0, stackBuffer);
        verifyFieldValues(tenantId, context, 1, 0, stackBuffer);
        verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, 1, stackBuffer).authz, 1, stackBuffer);
        verifyFieldValues(tenantId, context, 2, 0, stackBuffer);
        verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, 2, stackBuffer).authz, 2, stackBuffer);

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
        verifyFieldValues(tenantId, context, 3, 0, stackBuffer);
        verifyFieldValues(tenantId, context, 3, 1, stackBuffer);
        verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, 3, stackBuffer).authz, 3, stackBuffer);

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
        verifyFieldValues(tenantId, context, 4, 0, stackBuffer);
        verifyFieldValues(tenantId, context, 4, 2, stackBuffer);
        verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, 4, stackBuffer).authz, 4, stackBuffer);
    }

    @Test(dataProvider = "miruIndexContextDataProvider")
    public void testRepairData(MiruPartitionCoord coord,
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, ?> context,
        MiruIndexer<MutableRoaringBitmap, ImmutableRoaringBitmap> miruIndexer,
        List<MiruActivityAndId<MiruActivity>> activityList)
        throws Exception {

        StackBuffer stackBuffer = new StackBuffer();
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
            verifyFieldValues(tenantId, context, activityAndId.id, 0, stackBuffer);
            verifyFieldValues(tenantId, context, activityAndId.id, 1, stackBuffer);
            verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, activityAndId.id, stackBuffer).authz, activityAndId.id,
                stackBuffer);
        }

        // And check new data
        verifyFieldValues(tenantId, context, nextId, 0, stackBuffer);
        verifyFieldValues(tenantId, context, nextId, 1, stackBuffer);
        verifyAuthzValues(context.getAuthzIndex(), context.getActivityIndex().get(tenantId, nextId, stackBuffer).authz, nextId, stackBuffer);
    }

    private void verifyFieldValues(MiruTenantId tenantId,
        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, ?> context,
        int activityId,
        int fieldId,
        StackBuffer stackBuffer) throws Exception {

        MiruInternalActivity miruActivity = context.getActivityIndex().get(tenantId, activityId, stackBuffer);

        MiruTermId[] fieldValues = miruActivity.fieldsValues[fieldId];
        if (fieldValues == null) {
            fieldValues = new MiruTermId[0];
        }
        for (MiruTermId fieldValue : fieldValues) {
            MiruInvertedIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> invertedIndex = context.getFieldIndexProvider()
                .getFieldIndex(MiruFieldType.primary)
                .get(fieldId, fieldValue);
            assertNotNull(invertedIndex);
            ImmutableRoaringBitmap bitmap = invertedIndex.getIndex(stackBuffer).get();
            assertNotNull(bitmap);
            assertTrue(bitmap.contains(activityId));
        }
    }

    private void verifyAuthzValues(MiruAuthzIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> miruAuthzIndex,
        String[] authzs,
        int activityId,
        StackBuffer stackBuffer) throws Exception {
        MiruAuthzExpression miruAuthzExpression = new MiruAuthzExpression(Arrays.asList(authzs));

        ImmutableRoaringBitmap compositeAuthz = miruAuthzIndex.getCompositeAuthz(miruAuthzExpression, stackBuffer);
        assertTrue(compositeAuthz.contains(activityId));
    }

    @DataProvider(name = "miruIndexContextDataProvider")
    public Object[][] miruIndexContextDataProvider() throws Exception {
        final MiruTenantId tenantId = new MiruTenantId("indexContextTenant".getBytes());
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("localhost", 10000));

        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
        MiruIndexer<MutableRoaringBitmap, ImmutableRoaringBitmap> miruIndexer = new MiruIndexer<>(new MiruIndexAuthz<>(),
            new MiruIndexFieldValues<>(),
            new MiruIndexBloom<>(new BloomIndex<>(bitmaps, Hashing.murmur3_128(), 100_000, 0.01f)),
            new MiruIndexLatest<>(),
            new MiruIndexPairedLatest<>());

        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, ?> inMemoryContext = IndexTestUtil.buildInMemoryContext(4, bitmaps, coord);

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

        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, ?> onDiskContext = IndexTestUtil.buildOnDiskContext(4, bitmaps, coord);

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
