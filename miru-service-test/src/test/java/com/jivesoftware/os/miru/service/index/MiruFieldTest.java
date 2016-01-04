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
import com.jivesoftware.os.miru.bitmaps.roaring5.buffer.MiruBitmapsRoaringBuffer;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.BitmapAndLastId;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.service.IndexTestUtil;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import java.util.List;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
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
            Optional<BM> optional = fieldIndex.get("test", fieldId, new MiruTermId(FilerIO.intBytes(id))).getIndex(stackBuffer);
            assertTrue(optional.isPresent());
            assertEquals(bitmaps.cardinality(optional.get()), 1);
            assertTrue(bitmaps.isSet(optional.get(), id));
        }
    }

    @Test(dataProvider = "miruFieldDataProvider",
        enabled = true, description = "This test is disk dependent, disable if it flaps or becomes slow")
    public <BM extends IBM, IBM> void getInvertedIndexWithConsideration(MiruBitmaps<BM, IBM> bitmaps,
        MiruFieldIndex<BM, IBM> fieldIndex,
        int fieldId,
        List<Integer> ids)
        throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        // this works because maxId = id in our termToIndex maps
        int median = ids.get(ids.size() / 2);

        for (int id : ids) {
            Optional<BitmapAndLastId<BM>> optional = fieldIndex.get("test", fieldId, new MiruTermId(FilerIO.intBytes(id))).getIndexAndLastId(median,
                stackBuffer);
            assertEquals(optional.isPresent(), id > median, "Should be " + optional.isPresent() + ": " + id + " > " + median);
        }
    }

    @DataProvider(name = "miruFieldDataProvider")
    public Object[][] miruFieldDataProvider() throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        List<Integer> ids = Lists.newArrayList();
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
        MiruTenantId tenantId = new MiruTenantId(FilerIO.intBytes(1));
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("localhost", 10000));
        MiruFieldDefinition fieldDefinition = new MiruFieldDefinition(0, "field1", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE);

        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, ?> hybridContext = IndexTestUtil.buildInMemoryContext(4, bitmaps, coord);
        MiruFieldIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> hybridFieldIndex = hybridContext.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);

        for (int id = 0; id < 10; id++) {
            ids.add(id);
            hybridFieldIndex.append(fieldDefinition.fieldId, new MiruTermId(FilerIO.intBytes(id)), new int[] { id }, null, stackBuffer);
        }

        MiruContext<MutableRoaringBitmap, ImmutableRoaringBitmap, ?> onDiskContext = IndexTestUtil.buildOnDiskContext(4, bitmaps, coord);
        MiruFieldIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> onDiskFieldIndex = onDiskContext.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);

        for (int id = 0; id < 10; id++) {
            onDiskFieldIndex.append(fieldDefinition.fieldId, new MiruTermId(FilerIO.intBytes(id)), new int[] { id }, null, stackBuffer);
        }

        return new Object[][] {
            { bitmaps, hybridFieldIndex, fieldDefinition.fieldId, ids },
            { bitmaps, onDiskFieldIndex, fieldDefinition.fieldId, ids }
        };
    }
}
