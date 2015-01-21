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
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.service.IndexTestUtil;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
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
    public <BM> void getInvertedIndex(MiruBitmaps<BM> bitmaps, MiruFieldIndex<BM> fieldIndex, int fieldId, List<Integer> ids) throws Exception {
        for (int id : ids) {
            Optional<BM> optional = fieldIndex.get(fieldId, new MiruTermId(FilerIO.intBytes(id))).getIndex();
            assertTrue(optional.isPresent());
            assertEquals(bitmaps.cardinality(optional.get()), 1);
            assertTrue(bitmaps.isSet(optional.get(), id));
        }
    }

    @Test(dataProvider = "miruFieldDataProvider",
        enabled = true, description = "This test is disk dependent, disable if it flaps or becomes slow")
    public <BM> void getInvertedIndexWithConsideration(MiruBitmaps<BM> bitmaps, MiruFieldIndex<BM> fieldIndex, int fieldId, List<Integer> ids)
        throws Exception {
        // this works because maxId = id in our termToIndex maps
        int median = ids.get(ids.size() / 2);

        for (int id : ids) {
            Optional<BM> optional = fieldIndex.get(fieldId, new MiruTermId(FilerIO.intBytes(id)), median).getIndex();
            assertEquals(optional.isPresent(), id > median, "Should be " + optional.isPresent() + ": " + id + " > " + median);
        }
    }

    @DataProvider(name = "miruFieldDataProvider")
    public Object[][] miruFieldDataProvider() throws Exception {
        List<Integer> ids = Lists.newArrayList();
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(2);
        MiruTenantId tenantId = new MiruTenantId(FilerIO.intBytes(1));
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(0), new MiruHost("localhost", 10000));
        MiruFieldDefinition fieldDefinition = new MiruFieldDefinition(0, "field1");

        MiruContext<EWAHCompressedBitmap> hybridContext = IndexTestUtil.buildInMemoryContext(4, bitmaps, coord);
        MiruFieldIndex<EWAHCompressedBitmap> hybridFieldIndex = hybridContext.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);

        for (int id = 0; id < 10; id++) {
            ids.add(id);
            hybridFieldIndex.index(fieldDefinition.fieldId, new MiruTermId(FilerIO.intBytes(id)), id);
        }

        MiruContext<EWAHCompressedBitmap> onDiskContext = IndexTestUtil.buildOnDiskContext(4, bitmaps, coord);
        MiruFieldIndex<EWAHCompressedBitmap> onDiskFieldIndex = onDiskContext.fieldIndexProvider.getFieldIndex(MiruFieldType.primary);

        for (int id = 0; id < 10; id++) {
            onDiskFieldIndex.index(fieldDefinition.fieldId, new MiruTermId(FilerIO.intBytes(id)), id);
        }

        return new Object[][] {
            { bitmaps, hybridFieldIndex, fieldDefinition.fieldId, ids },
            { bitmaps, onDiskFieldIndex, fieldDefinition.fieldId, ids }
        };
    }
}
