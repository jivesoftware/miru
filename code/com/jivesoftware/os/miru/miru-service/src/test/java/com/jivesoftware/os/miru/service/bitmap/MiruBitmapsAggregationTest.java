package com.jivesoftware.os.miru.service.bitmap;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.query.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.query.MiruBitmaps;
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
        BM container = bitmaps.create();
        List<BM> ors = Lists.newArrayList();
        int numBits = 10;
        for (int i = 0; i < numBits; i++) {
            BM or = bitmaps.create();
            bitmaps.set(or, i * 137);
            ors.add(or);
        }
        bitmaps.or(container, ors);
        for (int i = 0; i < numBits; i++) {
            assertFalse(bitmaps.isSet(container, i * 137 - 1));
            assertTrue(bitmaps.isSet(container, i * 137));
            assertFalse(bitmaps.isSet(container, i * 137 + 1));
        }
    }

    @Test(dataProvider = "bitmapsProvider")
    public <BM> void testAnd(MiruBitmaps<BM> bitmaps) throws Exception {
        BM container = bitmaps.create();
        List<BM> ands = Lists.newArrayList();
        int numBits = 10;
        int andBits = 3;
        for (int i = 0; i < numBits - andBits; i++) {
            BM and = bitmaps.create();
            for (int j = i + 1; j < numBits; j++) {
                bitmaps.set(and, j * 137);
            }
            ands.add(and);
        }
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
        BM container = bitmaps.create();
        BM original = bitmaps.create();
        BM not = bitmaps.create();
        int numBits = 10;
        int notBits = 3;
        for (int i = 0; i < numBits; i++) {
            bitmaps.set(original, i * 137);
            if (i < notBits) {
                bitmaps.set(not, i * 137);
            }
        }
        bitmaps.andNot(container, original, Collections.singletonList(not));
        for (int i = 0; i < numBits; i++) {
            if (i < notBits) {
                assertFalse(bitmaps.isSet(container, i * 137));
            } else {
                assertTrue(bitmaps.isSet(container, i * 137));
            }
        }
    }

    @Test(dataProvider = "bitmapsProvider")
    public <BM> void testAndNot_multi(MiruBitmaps<BM> bitmaps) throws Exception {
        BM container = bitmaps.create();
        BM original = bitmaps.create();
        List<BM> nots = Lists.newArrayList();
        int numBits = 10;
        int notBits = 3;
        for (int i = 0; i < numBits; i++) {
            bitmaps.set(original, i * 137);
            if (i < notBits) {
                BM not = bitmaps.create();
                bitmaps.set(not, i * 137);
                nots.add(not);
            }
        }
        bitmaps.andNot(container, original, nots);
        for (int i = 0; i < numBits; i++) {
            if (i < notBits) {
                assertFalse(bitmaps.isSet(container, i * 137));
            } else {
                assertTrue(bitmaps.isSet(container, i * 137));
            }
        }
    }

    @Test(dataProvider = "bitmapsProvider")
    public <BM> void testAndNotWithCardinalityAndLastSetBit(MiruBitmaps<BM> bitmaps) throws Exception {
        BM container = bitmaps.create();
        BM original = bitmaps.create();
        BM not = bitmaps.create();
        int numBits = 10;
        int notBits = 3;
        for (int i = 0; i < numBits; i++) {
            bitmaps.set(original, i * 137);
            if (i < notBits) {
                bitmaps.set(not, i * 137);
            }
        }
        CardinalityAndLastSetBit cardinalityAndLastSetBit = bitmaps.andNotWithCardinalityAndLastSetBit(container, original, not);
        for (int i = 0; i < numBits; i++) {
            if (i < notBits) {
                assertFalse(bitmaps.isSet(container, i * 137));
            } else {
                assertTrue(bitmaps.isSet(container, i * 137));
            }
        }
        assertEquals(cardinalityAndLastSetBit.cardinality, numBits - notBits);
        assertEquals(cardinalityAndLastSetBit.lastSetBit, (numBits - 1) * 137);
    }

    @Test(dataProvider = "bitmapsProvider")
    public <BM> void testAndWithCardinalityAndLastSetBit(MiruBitmaps<BM> bitmaps) throws Exception {
        BM container = bitmaps.create();
        List<BM> ands = Lists.newArrayList();
        int numBits = 10;
        int andBits = 3;
        for (int i = 0; i < numBits - andBits; i++) {
            BM and = bitmaps.create();
            for (int j = i + 1; j < numBits; j++) {
                bitmaps.set(and, j * 137);
            }
            ands.add(and);
        }
        CardinalityAndLastSetBit cardinalityAndLastSetBit = bitmaps.andWithCardinalityAndLastSetBit(container, ands);
        for (int i = 0; i < numBits; i++) {
            if (i < (numBits - andBits)) {
                assertFalse(bitmaps.isSet(container, i * 137));
            } else {
                assertTrue(bitmaps.isSet(container, i * 137));
            }
        }
        assertEquals(cardinalityAndLastSetBit.cardinality, andBits);
        assertEquals(cardinalityAndLastSetBit.lastSetBit, (numBits - 1) * 137);
    }

    @DataProvider(name = "bitmapsProvider")
    public Object[][] bitmapsProvider() {
        return new Object[][] {
                new Object[] { new MiruBitmapsEWAH(1024) },
                new Object[] { new MiruBitmapsRoaring() }
        };
    }

}