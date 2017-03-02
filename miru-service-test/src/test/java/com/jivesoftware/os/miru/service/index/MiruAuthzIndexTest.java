package com.jivesoftware.os.miru.service.index;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.bitmaps.roaring6.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.IndexTestUtil.buildInMemoryContext;
import static com.jivesoftware.os.miru.service.IndexTestUtil.buildOnDiskContext;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class MiruAuthzIndexTest {

    @Test(dataProvider = "miruAuthzIndexDataProviderWithData")
    public void storeAndGetAuthz(MiruAuthzIndex<RoaringBitmap, RoaringBitmap> miruAuthzIndex,
        MiruAuthzUtils miruAuthzUtils,
        Map<String, List<Integer>> bitsIn) throws Exception {

        StackBuffer stackBuffer = new StackBuffer();
        for (Map.Entry<String, List<Integer>> entry : bitsIn.entrySet()) {
            String authz = entry.getKey();
            MiruAuthzExpression miruAuthzExpression = new MiruAuthzExpression(ImmutableList.of(authz));

            RoaringBitmap bitsOut = miruAuthzIndex.getCompositeAuthz(miruAuthzExpression, stackBuffer);
            List<Integer> actual = Lists.newArrayList();
            IntIterator iter = bitsOut.getIntIterator();
            while (iter.hasNext()) {
                actual.add(iter.next());
            }

            assertEquals(actual, entry.getValue());
        }
    }

    @DataProvider(name = "miruAuthzIndexDataProviderWithData")
    public Object[][] miruAuthzIndexDataProvider() throws Exception {
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(1), new MiruHost("logicalName"));
        MiruAuthzUtils<RoaringBitmap, RoaringBitmap> miruAuthzUtils = new MiruAuthzUtils<>(bitmaps);

        // kill me
        return ArrayUtils.addAll(buildAuthzDataProvider(bitmaps, coord, miruAuthzUtils, false),
            buildAuthzDataProvider(bitmaps, coord, miruAuthzUtils, true));
    }

    private Object[][] buildAuthzDataProvider(MiruBitmapsRoaring bitmaps,
        MiruPartitionCoord coord,
        MiruAuthzUtils<RoaringBitmap, RoaringBitmap> miruAuthzUtils,
        boolean useLabIndexes) throws Exception {

        MiruAuthzIndex<RoaringBitmap, RoaringBitmap> unmergedLargeMiruHybridAuthzIndex =
            buildInMemoryContext(4, useLabIndexes, true, bitmaps, coord).authzIndex;
        Map<String, List<Integer>> unmergedLargeHybridBitsIn = populateAuthzIndex(unmergedLargeMiruHybridAuthzIndex, miruAuthzUtils, 2);

        MiruAuthzIndex<RoaringBitmap, RoaringBitmap> unmergedLargeMiruOnDiskAuthzIndex =
            buildOnDiskContext(4, useLabIndexes, true, bitmaps, coord).authzIndex;
        Map<String, List<Integer>> unmergedLargeOnDiskBitsIn = populateAuthzIndex(unmergedLargeMiruOnDiskAuthzIndex, miruAuthzUtils, 2);

        return new Object[][] {
            { unmergedLargeMiruHybridAuthzIndex, miruAuthzUtils, unmergedLargeHybridBitsIn },
            { unmergedLargeMiruOnDiskAuthzIndex, miruAuthzUtils, unmergedLargeOnDiskBitsIn }
        };
    }

    private <BM extends IBM, IBM> Map<String, List<Integer>> populateAuthzIndex(MiruAuthzIndex<BM, IBM> authzIndex,
        MiruAuthzUtils<BM, IBM> miruAuthzUtils,
        int size) throws Exception {

        StackBuffer stackBuffer = new StackBuffer();
        //MiruSchema schema = new Builder("test", 1).build();
        Map<String, List<Integer>> bitsIn = Maps.newHashMap();

        for (int i = 1; i <= size; i++) {
            List<Integer> bits = Lists.newArrayList();
            bits.add(i);
            bits.add(10 * i);
            bits.add(100 * i);
            bits.add(1_000 * i);
            String authz = miruAuthzUtils.encode(FilerIO.longBytes((long) i));

            for (Integer bit : bits) {
                authzIndex.set(authz, stackBuffer, bit);
            }
            assertNull(bitsIn.put(authz, bits));
        }

        for (int i = 1; i <= size; i++) {
            List<Integer> bits = Lists.newArrayList();
            bits.add(10_000 * i);
            bits.add(100_000 * i);
            bits.add(1_000_000 * i);
            String authz = miruAuthzUtils.encode(FilerIO.longBytes((long) i));

            for (Integer bit : bits) {
                authzIndex.set(authz, stackBuffer, bit);
            }
            assertTrue(bitsIn.get(authz).addAll(bits));
        }

        return bitsIn;
    }
}
