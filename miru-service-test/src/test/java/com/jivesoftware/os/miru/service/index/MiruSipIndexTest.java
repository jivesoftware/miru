package com.jivesoftware.os.miru.service.index;

import com.google.common.base.Optional;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.wal.RCVSSipCursor;
import com.jivesoftware.os.miru.bitmaps.roaring6.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.index.MiruSipIndex;
import org.apache.commons.lang3.ArrayUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.IndexTestUtil.buildInMemoryContext;
import static com.jivesoftware.os.miru.service.IndexTestUtil.buildOnDiskContext;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class MiruSipIndexTest {

    @Test(dataProvider = "miruSipIndexDataProviderWithData")
    public void storeAndGetSip(MiruSipIndex<RCVSSipCursor> sipIndex, RCVSSipCursor expected) throws Exception {

        Optional<RCVSSipCursor> actual = sipIndex.getSip(new StackBuffer());
        assertTrue(actual.isPresent());
        assertEquals(actual.get(), expected);
    }

    @DataProvider(name = "miruSipIndexDataProviderWithData")
    public Object[][] miruSipIndexDataProviderWithData() throws Exception {
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(1), new MiruHost("logicalName"));

        return ArrayUtils.addAll(buildSipDataProvider(bitmaps, coord, false),
            buildSipDataProvider(bitmaps, coord, true));
    }

    private Object[][] buildSipDataProvider(MiruBitmapsRoaring bitmaps,
        MiruPartitionCoord coord,
        boolean useLabIndexes) throws Exception {

        RCVSSipCursor initial = new RCVSSipCursor((byte) 0, 1L, 2L, false);
        RCVSSipCursor expected = new RCVSSipCursor((byte) 1, 3L, 4L, true);

        MiruSipIndex<RCVSSipCursor> unmergedInMemorySipIndex = buildInMemoryContext(4, useLabIndexes, true, bitmaps, coord).sipIndex;
        populateSipIndex(unmergedInMemorySipIndex, initial, expected);

        MiruSipIndex<RCVSSipCursor> unmergedOnDiskSipIndex = buildOnDiskContext(4, useLabIndexes, true, bitmaps, coord).sipIndex;
        populateSipIndex(unmergedOnDiskSipIndex, initial, expected);

        return new Object[][] {
            { unmergedInMemorySipIndex, expected },
            { unmergedOnDiskSipIndex, expected }
        };
    }

    private void populateSipIndex(MiruSipIndex<RCVSSipCursor> sipIndex,
        RCVSSipCursor initial,
        RCVSSipCursor expected) throws Exception {

        StackBuffer stackBuffer = new StackBuffer();

        sipIndex.setSip(initial, stackBuffer);
        sipIndex.setSip(expected, stackBuffer);
    }
}
