package com.jivesoftware.os.miru.service.index;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah.IntIterator;
import com.jivesoftware.os.filer.io.FilerIO;
import com.jivesoftware.os.miru.api.MiruHost;
import com.jivesoftware.os.miru.api.MiruPartitionCoord;
import com.jivesoftware.os.miru.api.activity.MiruPartitionId;
import com.jivesoftware.os.miru.api.base.MiruTenantId;
import com.jivesoftware.os.miru.api.query.filter.MiruAuthzExpression;
import com.jivesoftware.os.miru.plugin.index.MiruAuthzIndex;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsEWAH;
import com.jivesoftware.os.miru.service.index.auth.MiruAuthzUtils;
import com.jivesoftware.os.miru.service.index.delta.MiruDeltaAuthzIndex;
import java.util.List;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.IndexTestUtil.buildInMemoryContext;
import static com.jivesoftware.os.miru.service.IndexTestUtil.buildOnDiskContext;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class MiruAuthzIndexTest {

    @Test(dataProvider = "miruAuthzIndexDataProviderWithData")
    public void storeAndGetAuthz(MiruAuthzIndex<EWAHCompressedBitmap> miruAuthzIndex, MiruAuthzUtils miruAuthzUtils, Map<String, List<Integer>> bitsIn)
        throws Exception {

        for (Map.Entry<String, List<Integer>> entry : bitsIn.entrySet()) {
            String authz = entry.getKey();
            MiruAuthzExpression miruAuthzExpression = new MiruAuthzExpression(ImmutableList.of(authz));

            EWAHCompressedBitmap bitsOut = miruAuthzIndex.getCompositeAuthz(miruAuthzExpression);
            List<Integer> actual = Lists.newArrayList();
            IntIterator iter = bitsOut.intIterator();
            while (iter.hasNext()) {
                actual.add(iter.next());
            }

            assertEquals(actual, entry.getValue());
        }
    }

    @DataProvider(name = "miruAuthzIndexDataProviderWithData")
    public Object[][] miruAuthzIndexDataProvider() throws Exception {
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(8_192);
        MiruTenantId tenantId = new MiruTenantId(new byte[] { 1 });
        MiruPartitionCoord coord = new MiruPartitionCoord(tenantId, MiruPartitionId.of(1), new MiruHost("localhost", 10000));
        MiruAuthzUtils<EWAHCompressedBitmap> miruAuthzUtils = new MiruAuthzUtils<>(bitmaps);

        MiruAuthzIndex<EWAHCompressedBitmap> unmergedLargeMiruHybridAuthzIndex = buildInMemoryContext(4, bitmaps, coord).authzIndex;
        Map<String, List<Integer>> unmergedLargeHybridBitsIn = populateAuthzIndex(unmergedLargeMiruHybridAuthzIndex, miruAuthzUtils, 2, false, false);

        MiruAuthzIndex<EWAHCompressedBitmap> unmergedLargeMiruOnDiskAuthzIndex = buildOnDiskContext(4, bitmaps, coord).authzIndex;
        Map<String, List<Integer>> unmergedLargeOnDiskBitsIn = populateAuthzIndex(unmergedLargeMiruOnDiskAuthzIndex, miruAuthzUtils, 2, false, false);

        MiruAuthzIndex<EWAHCompressedBitmap> mergedLargeMiruHybridAuthzIndex = buildInMemoryContext(4, bitmaps, coord).authzIndex;
        Map<String, List<Integer>> mergedLargeHybridBitsIn = populateAuthzIndex(mergedLargeMiruHybridAuthzIndex, miruAuthzUtils, 2, false, true);

        MiruAuthzIndex<EWAHCompressedBitmap> mergedLargeMiruOnDiskAuthzIndex = buildOnDiskContext(4, bitmaps, coord).authzIndex;
        Map<String, List<Integer>> mergedLargeOnDiskBitsIn = populateAuthzIndex(mergedLargeMiruOnDiskAuthzIndex, miruAuthzUtils, 2, false, true);

        MiruAuthzIndex<EWAHCompressedBitmap> partiallyMergedLargeMiruHybridAuthzIndex = buildInMemoryContext(4, bitmaps, coord).authzIndex;
        Map<String, List<Integer>> partiallyMergedLargeHybridBitsIn = populateAuthzIndex(partiallyMergedLargeMiruHybridAuthzIndex, miruAuthzUtils, 2,
            true, false);

        MiruAuthzIndex<EWAHCompressedBitmap> partiallyMergedLargeMiruOnDiskAuthzIndex = buildOnDiskContext(4, bitmaps, coord).authzIndex;
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

    private <BM> Map<String, List<Integer>> populateAuthzIndex(MiruAuthzIndex<EWAHCompressedBitmap> authzIndex,
        MiruAuthzUtils<BM> miruAuthzUtils,
        int size,
        boolean mergeMiddle,
        boolean mergeEnd)
        throws Exception {

        Map<String, List<Integer>> bitsIn = Maps.newHashMap();

        for (int i = 1; i <= size; i++) {
            List<Integer> bits = Lists.newArrayList();
            bits.add(i);
            bits.add(10 * i);
            bits.add(100 * i);
            bits.add(1_000 * i);
            String authz = miruAuthzUtils.encode(FilerIO.longBytes((long) i));

            for (Integer bit : bits) {
                authzIndex.append(authz, bit);
            }
            assertNull(bitsIn.put(authz, bits));
        }

        if (mergeMiddle) {
            ((MiruDeltaAuthzIndex<BM>) authzIndex).merge();
        }

        for (int i = 1; i <= size; i++) {
            List<Integer> bits = Lists.newArrayList();
            bits.add(10_000 * i);
            bits.add(100_000 * i);
            bits.add(1_000_000 * i);
            String authz = miruAuthzUtils.encode(FilerIO.longBytes((long) i));

            for (Integer bit : bits) {
                authzIndex.append(authz, bit);
            }
            assertTrue(bitsIn.get(authz).addAll(bits));
        }

        if (mergeEnd) {
            ((MiruDeltaAuthzIndex<BM>) authzIndex).merge();
        }

        return bitsIn;
    }
}
