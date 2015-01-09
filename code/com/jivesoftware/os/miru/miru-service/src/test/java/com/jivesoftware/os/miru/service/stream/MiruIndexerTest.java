package com.jivesoftware.os.miru.service.stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.api.MiruBackingStorage;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruActivity;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.DefaultMiruSchemaDefinition;
import com.jivesoftware.os.miru.api.base.MiruIBA;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.index.MiruActivityAndId;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInternalActivity;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.service.IndexTestUtil;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.index.BulkExport;
import com.jivesoftware.os.miru.service.index.BulkImport;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.fail;

public class MiruIndexerTest {

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
            MiruInvertedIndex<EWAHCompressedBitmap> invertedIndex = context.getFieldIndexProvider()
                .getFieldIndex(MiruFieldType.primary)
                .get(fieldId, new MiruTermId(fieldValue.getBytes()));
            assertNotNull(invertedIndex);
            EWAHCompressedBitmap bitmap = invertedIndex.getIndex().get();
            assertNotNull(bitmap);
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
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("localhost", 10000));

        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(4);
        MiruIndexer<EWAHCompressedBitmap> miruIndexer = new MiruIndexer<>(bitmaps);

        MiruContext<EWAHCompressedBitmap> hybridContext = IndexTestUtil.buildHybridContextAllocator(4, 10, true).allocate(bitmaps, coord);

        // Build in-memory index stream object
        MiruActivity miruActivity1 = buildMiruActivity(tenantId, 1, new String[] { "abcde" },
            ImmutableMap.of(DefaultMiruSchemaDefinition.FIELDS[0].name, "value1"));
        MiruActivity miruActivity2 = buildMiruActivity(tenantId, 2, new String[] { "fghij" },
            ImmutableMap.of(DefaultMiruSchemaDefinition.FIELDS[1].name, "value2"));
        MiruActivity miruActivity3 = buildMiruActivity(tenantId, 3, new String[] { "klmno" },
            ImmutableMap.of(DefaultMiruSchemaDefinition.FIELDS[2].name, "value3"));

        MiruContext<EWAHCompressedBitmap> onDiskContext = IndexTestUtil.buildOnDiskContextAllocator(4, 10).allocate(bitmaps, coord);

        // Index initial activities
        miruIndexer.index(hybridContext,
            Arrays.asList(
                new MiruActivityAndId<>(miruActivity1, 0),
                new MiruActivityAndId<>(miruActivity2, 1),
                new MiruActivityAndId<>(miruActivity3, 2)),
            MoreExecutors.sameThreadExecutor());

        ((BulkImport) onDiskContext.activityIndex).bulkImport(tenantId, (BulkExport) hybridContext.activityIndex);

        // Miru on-disk fields
        for (MiruFieldType fieldType : MiruFieldType.values()) {
            ((BulkImport) onDiskContext.fieldIndexProvider.getFieldIndex(fieldType))
                .bulkImport(tenantId, (BulkExport) hybridContext.fieldIndexProvider.getFieldIndex(fieldType));
        }

        ((BulkImport) onDiskContext.authzIndex).bulkImport(tenantId, (BulkExport) hybridContext.authzIndex);

        return new Object[][] {
            { tenantId, hybridContext, miruIndexer, MiruBackingStorage.memory },
            { tenantId, onDiskContext, miruIndexer, MiruBackingStorage.disk }
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
