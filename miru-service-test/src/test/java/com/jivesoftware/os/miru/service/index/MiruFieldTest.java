package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.api.field.MiruFieldType;
import com.jivesoftware.os.miru.bitmaps.ewah.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.service.IndexTestUtil;
import com.jivesoftware.os.miru.service.stream.MiruContext;
import java.util.List;
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
    public <BM extends IBM, IBM> void getInvertedIndex(MiruBitmaps<BM, IBM> bitmaps, MiruFieldIndex<BM> fieldIndex, int fieldId, List<Integer> ids) throws Exception {
        byte[] primitiveBuffer = new byte[8];
        for (int id : ids) {
            Optional<BM> optional = fieldIndex.get(fieldId, new MiruTermId(FilerIO.intBytes(id))).getIndex(primitiveBuffer);
            assertTrue(optional.isPresent());
            assertEquals(bitmaps.cardinality(optional.get()), 1);
            assertTrue(bitmaps.isSet(optional.get(), id));
        }
    }

    @Test(dataProvider = "miruFieldDataProvider",
        enabled = true, description = "This test is disk dependent, disable if it flaps or becomes slow")
    public <BM extends IBM, IBM> void getInvertedIndexWithConsideration(MiruBitmaps<BM, IBM> bitmaps, MiruFieldIndex<BM> fieldIndex, int fieldId, List<Integer> ids)
        throws Exception {
        byte[] primitiveBuffer = new byte[8];
        // this works because maxId = id in our termToIndex maps
        int median = ids.get(ids.size() / 2);

        for (int id : ids) {
            Optional<BM> optional = fieldIndex.get(fieldId, new MiruTermId(FilerIO.intBytes(id)), median).getIndex(primitiveBuffer);
            assertEquals(optional.isPresent(), id > median, "Should be " + optional.isPresent() + ": " + id + " > " + median);
        }
    }

    @DataProvider(name = "miruFieldDataProvider")
    public Object[][] miruFieldDataProvider() throws Exception {
        byte[] primitiveBuffer = new byte[8];
        List<Integer> ids = Lists.newArrayList();
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(2);
        MiruTenantId tenantId = new MiruTenantId(FilerIO.intBytes(1));
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("localhost", 10000));
        MiruFieldDefinition fieldDefinition = new MiruFieldDefinition(0, "field1", MiruFieldDefinition.Type.singleTerm, MiruFieldDefinition.Prefix.NONE);

        MiruContext<EWAHCompressedBitmap, ?> hybridContext = IndexTestUtil.buildInMemoryContext(4, bitmaps, coord);
        MiruFieldIndex<EWAHCompressedBitmap> hybridFieldIndex = hybridContext.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);

        for (int id = 0; id < 10; id++) {
            ids.add(id);
            hybridFieldIndex.append(fieldDefinition.fieldId, new MiruTermId(FilerIO.intBytes(id)), new int[]{id}, null, primitiveBuffer);
        }

        MiruContext<EWAHCompressedBitmap, ?> onDiskContext = IndexTestUtil.buildOnDiskContext(4, bitmaps, coord);
        MiruFieldIndex<EWAHCompressedBitmap> onDiskFieldIndex = onDiskContext.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);

        for (int id = 0; id < 10; id++) {
            onDiskFieldIndex.append(fieldDefinition.fieldId, new MiruTermId(FilerIO.intBytes(id)), new int[]{id}, null, primitiveBuffer);
        }

        return new Object[][]{
            {bitmaps, hybridFieldIndex, fieldDefinition.fieldId, ids},
            {bitmaps, onDiskFieldIndex, fieldDefinition.fieldId, ids}
        };
    }
}
