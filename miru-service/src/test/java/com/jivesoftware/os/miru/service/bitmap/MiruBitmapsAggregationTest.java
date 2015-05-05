package com.jivesoftware.os.miru.service.bitmap;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import gnu.trove.list.array.TIntArrayList;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class MiruBitmapsAggregationTest {

    @Test(dataProvider = "bitmapsProvider")
    public <BM> void testOr(MiruBitmaps<BM> bitmaps) throws Exception {
        List<BM> ors = Lists.newArrayList();
        int numBits = 10;
        for (int i = 0; i < numBits; i++) {
            BM or = bitmaps.createWithBits(i * 137);
            ors.add(or);
        }
        BM container = bitmaps.create();
        bitmaps.or(container, ors);
        for (int i = 0; i < numBits; i++) {
            assertFalse(bitmaps.isSet(container, i * 137 - 1));
            assertTrue(bitmaps.isSet(container, i * 137));
            assertFalse(bitmaps.isSet(container, i * 137 + 1));
        }
    }

    @Test(dataProvider = "bitmapsProvider")
    public <BM> void testAnd(MiruBitmaps<BM> bitmaps) throws Exception {
        List<BM> ands = Lists.newArrayList();
        int numBits = 10;
        int andBits = 3;
        for (int i = 0; i < numBits - andBits; i++) {
            TIntArrayList bits = new TIntArrayList();
            for (int j = i + 1; j < numBits; j++) {
                bits.add(j * 137);
            }
            ands.add(bitmaps.createWithBits(bits.toArray()));
        }
        BM container = bitmaps.create();
        bitmaps.and(container, ands);
        for (int i = 0; i < numBits; i++) {
            if (i < (numBits - andBits)) {
                assertFalse(bitmaps.isSet(container, i * 137));
            } else {
                assertTrue(bitmaps.isSet(container, i * 137));
            }
        }
    }

    @Test(dataProvider = "bitmapsProvider")
    public <BM> void testAndNot_2(MiruBitmaps<BM> bitmaps) throws Exception {
        int numOriginal = 10;
        int numNot = 3;
        TIntArrayList originalBits = new TIntArrayList();
        TIntArrayList notBits = new TIntArrayList();
        for (int i = 0; i < numOriginal; i++) {
            originalBits.add(i * 137);
            if (i < numNot) {
                notBits.add(i * 137);
            }
        }
        BM original = bitmaps.createWithBits(originalBits.toArray());
        BM not = bitmaps.createWithBits(notBits.toArray());
        BM container = bitmaps.create();
        bitmaps.andNot(container, original, Collections.singletonList(not));
        for (int i = 0; i < numOriginal; i++) {
            if (i < numNot) {
                assertFalse(bitmaps.isSet(container, i * 137));
            } else {
                assertTrue(bitmaps.isSet(container, i * 137));
            }
        }
    }

    @Test(dataProvider = "bitmapsProvider")
    public <BM> void testAndNot_multi(MiruBitmaps<BM> bitmaps) throws Exception {
        List<BM> nots = Lists.newArrayList();
        int numOriginal = 10;
        int numNot = 3;
        TIntArrayList originalBits = new TIntArrayList();
        for (int i = 0; i < numOriginal; i++) {
            originalBits.add(i * 137);
            if (i < numNot) {
                BM not = bitmaps.createWithBits(i * 137);
                nots.add(not);
            }
        }
        BM original = bitmaps.createWithBits(originalBits.toArray());
        BM container = bitmaps.create();
        bitmaps.andNot(container, original, nots);
        for (int i = 0; i < numOriginal; i++) {
            if (i < numNot) {
                assertFalse(bitmaps.isSet(container, i * 137));
            } else {
                assertTrue(bitmaps.isSet(container, i * 137));
            }
        }
    }

    @Test(dataProvider = "bitmapsProvider")
    public <BM> void testAndNotWithCardinalityAndLastSetBit(MiruBitmaps<BM> bitmaps) throws Exception {
        int numOriginal = 10;
        int numNot = 3;
        TIntArrayList originalBits = new TIntArrayList();
        TIntArrayList notBits = new TIntArrayList();
        for (int i = 0; i < numOriginal; i++) {
            originalBits.add(i * 137);
            if (i < numNot) {
                notBits.add(i * 137);
            }
        }
        BM original = bitmaps.createWithBits(originalBits.toArray());
        BM not = bitmaps.createWithBits(notBits.toArray());
        BM container = bitmaps.create();
        CardinalityAndLastSetBit cardinalityAndLastSetBit = bitmaps.andNotWithCardinalityAndLastSetBit(container, original, not);
        for (int i = 0; i < numOriginal; i++) {
            if (i < numNot) {
                assertFalse(bitmaps.isSet(container, i * 137));
            } else {
                assertTrue(bitmaps.isSet(container, i * 137));
            }
        }
        assertEquals(cardinalityAndLastSetBit.cardinality, numOriginal - numNot);
        assertEquals(cardinalityAndLastSetBit.lastSetBit, (numOriginal - 1) * 137);
    }

    @Test(dataProvider = "bitmapsProvider")
    public <BM> void testAndWithCardinalityAndLastSetBit(MiruBitmaps<BM> bitmaps) throws Exception {
        List<BM> ands = Lists.newArrayList();
        int numOriginal = 10;
        int numAnd = 3;
        for (int i = 0; i < numOriginal - numAnd; i++) {
            TIntArrayList andBits = new TIntArrayList();
            for (int j = i + 1; j < numOriginal; j++) {
                andBits.add(j * 137);
            }
            ands.add(bitmaps.createWithBits(andBits.toArray()));
        }
        BM container = bitmaps.create();
        CardinalityAndLastSetBit cardinalityAndLastSetBit = bitmaps.andWithCardinalityAndLastSetBit(container, ands);
        for (int i = 0; i < numOriginal; i++) {
            if (i < (numOriginal - numAnd)) {
                assertFalse(bitmaps.isSet(container, i * 137));
            } else {
                assertTrue(bitmaps.isSet(container, i * 137));
            }
        }
        assertEquals(cardinalityAndLastSetBit.cardinality, numAnd);
        assertEquals(cardinalityAndLastSetBit.lastSetBit, (numOriginal - 1) * 137);
    }

    @DataProvider(name = "bitmapsProvider")
    public Object[][] bitmapsProvider() {
        return new Object[][] {
                new Object[] { new MiruBitmapsEWAH(1_024) },
                new Object[] { new MiruBitmapsRoaring() }
        };
    }

}
