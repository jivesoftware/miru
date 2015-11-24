package com.jivesoftware.os.miru.bitmaps.ewah;

import com.google.common.collect.Lists;
import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import gnu.trove.list.array.TIntArrayList;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class MiruBitmapsAggregationTest {

    @Test
    public void testOr() throws Exception {
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(1_024);
        List<EWAHCompressedBitmap> ors = Lists.newArrayList();
        int numBits = 10;
        for (int i = 0; i < numBits; i++) {
            EWAHCompressedBitmap or = bitmaps.createWithBits(i * 137);
            ors.add(or);
        }
        EWAHCompressedBitmap container = bitmaps.create();
        bitmaps.or(container, ors);
        for (int i = 0; i < numBits; i++) {
            assertFalse(bitmaps.isSet(container, i * 137 - 1));
            assertTrue(bitmaps.isSet(container, i * 137));
            assertFalse(bitmaps.isSet(container, i * 137 + 1));
        }
    }

    @Test
    public void testAnd() throws Exception {
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(1_024);
        List<EWAHCompressedBitmap> ands = Lists.newArrayList();
        int numBits = 10;
        int andBits = 3;
        for (int i = 0; i < numBits - andBits; i++) {
            TIntArrayList bits = new TIntArrayList();
            for (int j = i + 1; j < numBits; j++) {
                bits.add(j * 137);
            }
            ands.add(bitmaps.createWithBits(bits.toArray()));
        }
        EWAHCompressedBitmap container = bitmaps.create();
        bitmaps.and(container, ands);
        for (int i = 0; i < numBits; i++) {
            if (i < (numBits - andBits)) {
                assertFalse(bitmaps.isSet(container, i * 137));
            } else {
                assertTrue(bitmaps.isSet(container, i * 137));
            }
        }
    }

    @Test
    public void testAndNot_2() throws Exception {
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(1_024);
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
        EWAHCompressedBitmap original = bitmaps.createWithBits(originalBits.toArray());
        EWAHCompressedBitmap not = bitmaps.createWithBits(notBits.toArray());
        EWAHCompressedBitmap container = bitmaps.create();
        bitmaps.andNot(container, original, not);
        for (int i = 0; i < numOriginal; i++) {
            if (i < numNot) {
                assertFalse(bitmaps.isSet(container, i * 137));
            } else {
                assertTrue(bitmaps.isSet(container, i * 137));
            }
        }
    }

    public void testAndNot_multi() throws Exception {
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(1_024);
        List<EWAHCompressedBitmap> nots = Lists.newArrayList();
        int numOriginal = 10;
        int numNot = 3;
        TIntArrayList originalBits = new TIntArrayList();
        for (int i = 0; i < numOriginal; i++) {
            originalBits.add(i * 137);
            if (i < numNot) {
                EWAHCompressedBitmap not = bitmaps.createWithBits(i * 137);
                nots.add(not);
            }
        }
        EWAHCompressedBitmap original = bitmaps.createWithBits(originalBits.toArray());
        EWAHCompressedBitmap container = bitmaps.create();
        bitmaps.andNot(container, original, nots);
        for (int i = 0; i < numOriginal; i++) {
            if (i < numNot) {
                assertFalse(bitmaps.isSet(container, i * 137));
            } else {
                assertTrue(bitmaps.isSet(container, i * 137));
            }
        }
    }

    public void testAndNotWithCardinalityAndLastSetBit() throws Exception {
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(1_024);
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
        EWAHCompressedBitmap original = bitmaps.createWithBits(originalBits.toArray());
        EWAHCompressedBitmap not = bitmaps.createWithBits(notBits.toArray());
        EWAHCompressedBitmap container = bitmaps.create();
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

    public void testAndWithCardinalityAndLastSetBit() throws Exception {
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(1_024);
        List<EWAHCompressedBitmap> ands = Lists.newArrayList();
        int numOriginal = 10;
        int numAnd = 3;
        for (int i = 0; i < numOriginal - numAnd; i++) {
            TIntArrayList andBits = new TIntArrayList();
            for (int j = i + 1; j < numOriginal; j++) {
                andBits.add(j * 137);
            }
            ands.add(bitmaps.createWithBits(andBits.toArray()));
        }
        EWAHCompressedBitmap container = bitmaps.create();
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

}
