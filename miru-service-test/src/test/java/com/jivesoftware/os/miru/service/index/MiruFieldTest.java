package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.bitmaps.roaring5.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.service.IndexTestUtil;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 *
 */
public class MiruFieldTest {

    @Test(dataProvider = "miruFieldDataProvider",
        enabled = true, description = "This test is disk dependent, disable if it flaps or becomes slow")
    public <BM extends IBM, IBM> void getInvertedIndex(MiruBitmaps<BM, IBM> bitmaps,
        MiruFieldIndex<BM, IBM> fieldIndex,
        int fieldId,
        List<Integer> ids) throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        for (int id : ids) {
            BitmapAndLastId<BM> container = new BitmapAndLastId<>();
            fieldIndex.get("test", fieldId, new MiruTermId(FilerIO.intBytes(id))).getIndex(container, stackBuffer);
            assertTrue(container.isSet());
            assertEquals(bitmaps.cardinality(container.getBitmap()), 1);
            assertTrue(bitmaps.isSet(container.getBitmap(), id));
        }
    }

    @DataProvider(name = "miruFieldDataProvider")
    public Object[][] miruFieldDataProvider() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruTenantId tenantId = new MiruTenantId("tenantId".getBytes(StandardCharsets.UTF_8));
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("logicalName"));
        MiruFieldDefinition fieldDefinition = new MiruFieldDefinition(0, "field1", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE);

        return ArrayUtils.addAll(buildFieldDataProvider(stackBuffer, bitmaps, coord, fieldDefinition, false),
            buildFieldDataProvider(stackBuffer, bitmaps, coord, fieldDefinition, true));
    }

    private Object[][] buildFieldDataProvider(StackBuffer stackBuffer,
        MiruBitmapsRoaring bitmaps,
        MiruPartitionCoord coord,
        MiruFieldDefinition fieldDefinition,
        boolean useLabIndexes) throws Exception {

        MiruContext<RoaringBitmap, RoaringBitmap, ?> hybridContext = IndexTestUtil.buildInMemoryContext(4, useLabIndexes, true, bitmaps, coord);
        MiruFieldIndex<RoaringBitmap, RoaringBitmap> hybridFieldIndex = hybridContext.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);

        List<Integer> ids = Lists.newArrayList();
        for (int id = 0; id < 10; id++) {
            ids.add(id);
            hybridFieldIndex.set(fieldDefinition.fieldId, new MiruTermId(FilerIO.intBytes(id)), new int[] { id }, null, stackBuffer);
        }

        MiruContext<RoaringBitmap, RoaringBitmap, ?> onDiskContext = IndexTestUtil.buildOnDiskContext(4, useLabIndexes, true, bitmaps, coord);
        MiruFieldIndex<RoaringBitmap, RoaringBitmap> onDiskFieldIndex = onDiskContext.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);

        for (int id = 0; id < 10; id++) {
            onDiskFieldIndex.set(fieldDefinition.fieldId, new MiruTermId(FilerIO.intBytes(id)), new int[] { id }, null, stackBuffer);
        }

        return new Object[][] {
            { bitmaps, hybridFieldIndex, fieldDefinition.fieldId, ids },
            { bitmaps, onDiskFieldIndex, fieldDefinition.fieldId, ids }
        };
    }
}
