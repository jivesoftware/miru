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
import java.util.List;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.jivesoftware.os.miru.service.IndexTestUtil.buildInMemoryContext;
import static com.jivesoftware.os.miru.service.IndexTestUtil.buildOnDiskContext;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

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

        // Create in-memory authz index
        MiruAuthzIndex<EWAHCompressedBitmap> largeMiruHybridAuthzIndex = buildInMemoryContext(4, bitmaps, coord).authzIndex;
        Map<String, List<Integer>> largeHybridBitsIn = populateAuthzIndex(largeMiruHybridAuthzIndex, miruAuthzUtils, 2);

        MiruAuthzIndex<EWAHCompressedBitmap> largeMiruOnDiskAuthzIndex = buildOnDiskContext(4, bitmaps, coord).authzIndex;
        Map<String, List<Integer>> largeOnDiskBitsIn = populateAuthzIndex(largeMiruOnDiskAuthzIndex, miruAuthzUtils, 2);

        return new Object[][] {
            { largeMiruHybridAuthzIndex, miruAuthzUtils, largeHybridBitsIn },
            { largeMiruOnDiskAuthzIndex, miruAuthzUtils, largeOnDiskBitsIn }
        };
    }

    private <BM> Map<String, List<Integer>> populateAuthzIndex(MiruAuthzIndex<EWAHCompressedBitmap> authzIndex,
        MiruAuthzUtils<BM> miruAuthzUtils,
        int size)
        throws Exception {

        Map<String, List<Integer>> bitsIn = Maps.newHashMap();

        for (int i = 1; i <= size; i++) {
            List<Integer> bits = Lists.newArrayList();
            bits.add(i);
            bits.add(10 * i);
            bits.add(100 * i);
            bits.add(1_000 * i);
            bits.add(10_000 * i);
            bits.add(100_000 * i);
            bits.add(1_000_000 * i);
            String authz = miruAuthzUtils.encode(FilerIO.longBytes((long) i));

            for (Integer bit : bits) {
                authzIndex.index(authz, bit);
            }
            assertNull(bitsIn.put(authz, bits));
        }
        return bitsIn;
    }
}
