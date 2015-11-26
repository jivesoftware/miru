package com.jivesoftware.os.miru.bitmaps.roaring5.buffer;

import com.google.common.collect.Lists;
import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import gnu.trove.list.array.TIntArrayList;
import java.util.List;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class MiruBitmapsAggregationTest {

    @Test
    public void testOr() throws Exception {
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
        List<ImmutableRoaringBitmap> ors = Lists.newArrayList();
        int numBits = 10;
        for (int i = 0; i < numBits; i++) {
            MutableRoaringBitmap or = bitmaps.createWithBits(i * 137);
            ors.add(or);
        }
        MutableRoaringBitmap container = bitmaps.or(ors);
        for (int i = 0; i < numBits; i++) {
            assertFalse(bitmaps.isSet(container, i * 137 - 1));
            assertTrue(bitmaps.isSet(container, i * 137));
            assertFalse(bitmaps.isSet(container, i * 137 + 1));
        }
    }

    @Test
    public void testAnd() throws Exception {
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
        List<ImmutableRoaringBitmap> ands = Lists.newArrayList();
        int numBits = 10;
        int andBits = 3;
        for (int i = 0; i < numBits - andBits; i++) {
            TIntArrayList bits = new TIntArrayList();
            for (int j = i + 1; j < numBits; j++) {
                bits.add(j * 137);
            }
            ands.add(bitmaps.createWithBits(bits.toArray()));
        }
        MutableRoaringBitmap container = bitmaps.and(ands);
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
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
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
        MutableRoaringBitmap original = bitmaps.createWithBits(originalBits.toArray());
        MutableRoaringBitmap not = bitmaps.createWithBits(notBits.toArray());
        MutableRoaringBitmap container = bitmaps.andNot(original, not);
        for (int i = 0; i < numOriginal; i++) {
            if (i < numNot) {
                assertFalse(bitmaps.isSet(container, i * 137));
            } else {
                assertTrue(bitmaps.isSet(container, i * 137));
            }
        }
    }

    @Test
    public void testAndNot_multi() throws Exception {
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
        List<ImmutableRoaringBitmap> nots = Lists.newArrayList();
        int numOriginal = 10;
        int numNot = 3;
        TIntArrayList originalBits = new TIntArrayList();
        for (int i = 0; i < numOriginal; i++) {
            originalBits.add(i * 137);
            if (i < numNot) {
                MutableRoaringBitmap not = bitmaps.createWithBits(i * 137);
                nots.add(not);
            }
        }
        MutableRoaringBitmap original = bitmaps.createWithBits(originalBits.toArray());
        MutableRoaringBitmap container = bitmaps.andNot(original, nots);
        for (int i = 0; i < numOriginal; i++) {
            if (i < numNot) {
                assertFalse(bitmaps.isSet(container, i * 137));
            } else {
                assertTrue(bitmaps.isSet(container, i * 137));
            }
        }
    }

    @Test
    public void testAndNotWithCardinalityAndLastSetBit() throws Exception {
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
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
        MutableRoaringBitmap original = bitmaps.createWithBits(originalBits.toArray());
        MutableRoaringBitmap not = bitmaps.createWithBits(notBits.toArray());
        CardinalityAndLastSetBit<MutableRoaringBitmap> cardinalityAndLastSetBit = bitmaps.andNotWithCardinalityAndLastSetBit(original, not);
        for (int i = 0; i < numOriginal; i++) {
            if (i < numNot) {
                assertFalse(bitmaps.isSet(cardinalityAndLastSetBit.bitmap, i * 137));
            } else {
                assertTrue(bitmaps.isSet(cardinalityAndLastSetBit.bitmap, i * 137));
            }
        }
        assertEquals(cardinalityAndLastSetBit.cardinality, numOriginal - numNot);
        assertEquals(cardinalityAndLastSetBit.lastSetBit, (numOriginal - 1) * 137);
    }

    @Test
    public void testAndWithCardinalityAndLastSetBit() throws Exception {
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
        List<ImmutableRoaringBitmap> ands = Lists.newArrayList();
        int numOriginal = 10;
        int numAnd = 3;
        for (int i = 0; i < numOriginal - numAnd; i++) {
            TIntArrayList andBits = new TIntArrayList();
            for (int j = i + 1; j < numOriginal; j++) {
                andBits.add(j * 137);
            }
            ands.add(bitmaps.createWithBits(andBits.toArray()));
        }
        CardinalityAndLastSetBit<MutableRoaringBitmap> cardinalityAndLastSetBit = bitmaps.andWithCardinalityAndLastSetBit(ands);
        for (int i = 0; i < numOriginal; i++) {
            if (i < (numOriginal - numAnd)) {
                assertFalse(bitmaps.isSet(cardinalityAndLastSetBit.bitmap, i * 137));
            } else {
                assertTrue(bitmaps.isSet(cardinalityAndLastSetBit.bitmap, i * 137));
            }
        }
        assertEquals(cardinalityAndLastSetBit.cardinality, numAnd);
        assertEquals(cardinalityAndLastSetBit.lastSetBit, (numOriginal - 1) * 137);
    }

}
