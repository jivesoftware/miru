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
import com.jivesoftware.os.miru.bitmaps.roaring5.buffer.MiruBitmapsRoaringBuffer;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaAuthzIndex;
import java.util.List;
import java.util.Map;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.IndexTestUtil.buildInMemoryContext;
import static com.jivesoftware.os.miru.service.IndexTestUtil.buildOnDiskContext;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class MiruAuthzIndexTest {

    @Test(dataProvider = "miruAuthzIndexDataProviderWithData")
    public void storeAndGetAuthz(MiruAuthzIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> miruAuthzIndex,
        MiruAuthzUtils miruAuthzUtils,
        Map<String, List<Integer>> bitsIn) throws Exception {

        StackBuffer stackBuffer = new StackBuffer();
        for (Map.Entry<String, List<Integer>> entry : bitsIn.entrySet()) {
            String authz = entry.getKey();
            MiruAuthzExpression miruAuthzExpression = new MiruAuthzExpression(ImmutableList.of(authz));

            ImmutableRoaringBitmap bitsOut = miruAuthzIndex.getCompositeAuthz(miruAuthzExpression, stackBuffer);
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
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(1), new MiruHost("localhost", 10000));
        MiruAuthzUtils<MutableRoaringBitmap, ImmutableRoaringBitmap> miruAuthzUtils = new MiruAuthzUtils<>(bitmaps);

        MiruAuthzIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> unmergedLargeMiruHybridAuthzIndex = buildInMemoryContext(4, bitmaps, coord).authzIndex;
        Map<String, List<Integer>> unmergedLargeHybridBitsIn = populateAuthzIndex(unmergedLargeMiruHybridAuthzIndex, miruAuthzUtils, 2, false, false);

        MiruAuthzIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> unmergedLargeMiruOnDiskAuthzIndex = buildOnDiskContext(4, bitmaps, coord).authzIndex;
        Map<String, List<Integer>> unmergedLargeOnDiskBitsIn = populateAuthzIndex(unmergedLargeMiruOnDiskAuthzIndex, miruAuthzUtils, 2, false, false);

        MiruAuthzIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> mergedLargeMiruHybridAuthzIndex = buildInMemoryContext(4, bitmaps, coord).authzIndex;
        Map<String, List<Integer>> mergedLargeHybridBitsIn = populateAuthzIndex(mergedLargeMiruHybridAuthzIndex, miruAuthzUtils, 2, false, true);

        MiruAuthzIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> mergedLargeMiruOnDiskAuthzIndex = buildOnDiskContext(4, bitmaps, coord).authzIndex;
        Map<String, List<Integer>> mergedLargeOnDiskBitsIn = populateAuthzIndex(mergedLargeMiruOnDiskAuthzIndex, miruAuthzUtils, 2, false, true);

        MiruAuthzIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> partiallyMergedLargeMiruHybridAuthzIndex = buildInMemoryContext(4, bitmaps,
            coord).authzIndex;
        Map<String, List<Integer>> partiallyMergedLargeHybridBitsIn = populateAuthzIndex(partiallyMergedLargeMiruHybridAuthzIndex, miruAuthzUtils, 2,
            true, false);

        MiruAuthzIndex<MutableRoaringBitmap, ImmutableRoaringBitmap> partiallyMergedLargeMiruOnDiskAuthzIndex = buildOnDiskContext(4, bitmaps,
            coord).authzIndex;
        Map<String, List<Integer>> partiallyMergedLargeOnDiskBitsIn = populateAuthzIndex(partiallyMergedLargeMiruOnDiskAuthzIndex, miruAuthzUtils, 2,
            true, false);

        return new Object[][] {
            { unmergedLargeMiruHybridAuthzIndex, miruAuthzUtils, unmergedLargeHybridBitsIn },
            { unmergedLargeMiruOnDiskAuthzIndex, miruAuthzUtils, unmergedLargeOnDiskBitsIn },
            { mergedLargeMiruHybridAuthzIndex, miruAuthzUtils, mergedLargeHybridBitsIn },
            { mergedLargeMiruOnDiskAuthzIndex, miruAuthzUtils, mergedLargeOnDiskBitsIn },
            { partiallyMergedLargeMiruHybridAuthzIndex, miruAuthzUtils, partiallyMergedLargeHybridBitsIn },
            { partiallyMergedLargeMiruOnDiskAuthzIndex, miruAuthzUtils, partiallyMergedLargeOnDiskBitsIn }
        };
    }

    private <BM extends IBM, IBM> Map<String, List<Integer>> populateAuthzIndex(MiruAuthzIndex<BM, IBM> authzIndex,
        MiruAuthzUtils<BM, IBM> miruAuthzUtils,
        int size,
        boolean mergeMiddle,
        boolean mergeEnd)
        throws Exception {
        StackBuffer stackBuffer = new StackBuffer();
        Map<String, List<Integer>> bitsIn = Maps.newHashMap();

        for (int i = 1; i <= size; i++) {
            List<Integer> bits = Lists.newArrayList();
            bits.add(i);
            bits.add(10 * i);
            bits.add(100 * i);
            bits.add(1_000 * i);
            String authz = miruAuthzUtils.encode(FilerIO.longBytes((long) i));

            for (Integer bit : bits) {
                authzIndex.append(authz, stackBuffer, bit);
            }
            assertNull(bitsIn.put(authz, bits));
        }

        if (mergeMiddle) {
            ((MiruDeltaAuthzIndex<BM, IBM>) authzIndex).merge(stackBuffer);
        }

        for (int i = 1; i <= size; i++) {
            List<Integer> bits = Lists.newArrayList();
            bits.add(10_000 * i);
            bits.add(100_000 * i);
            bits.add(1_000_000 * i);
            String authz = miruAuthzUtils.encode(FilerIO.longBytes((long) i));

            for (Integer bit : bits) {
                authzIndex.append(authz, stackBuffer, bit);
            }
            assertTrue(bitsIn.get(authz).addAll(bits));
        }

        if (mergeEnd) {
            ((MiruDeltaAuthzIndex<BM, IBM>) authzIndex).merge(stackBuffer);
        }

        return bitsIn;
    }
}
