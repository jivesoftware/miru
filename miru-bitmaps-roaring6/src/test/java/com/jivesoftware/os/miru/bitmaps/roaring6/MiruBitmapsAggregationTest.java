package com.jivesoftware.os.miru.bitmaps.roaring6;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.jivesoftware.os.filer.io.api.StackBuffer;
import com.jivesoftware.os.miru.bitmaps.roaring6.MiruBitmapsRoaring;
import com.jivesoftware.os.miru.plugin.bitmap.CardinalityAndLastSetBit;
import gnu.trove.list.array.TIntArrayList;
import java.util.List;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class MiruBitmapsAggregationTest {

    @Test
    public void testOr() throws Exception {
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        List<RoaringBitmap> ors = Lists.newArrayList();
        int numBits = 10;
        for (int i = 0; i < numBits; i++) {
            RoaringBitmap or = bitmaps.createWithBits(i * 137);
            ors.add(or);
        }
        RoaringBitmap container = bitmaps.or(ors);
        for (int i = 0; i < numBits; i++) {
            assertFalse(bitmaps.isSet(container, i * 137 - 1));
            assertTrue(bitmaps.isSet(container, i * 137));
            assertFalse(bitmaps.isSet(container, i * 137 + 1));
        }
    }

    @Test
    public void testAnd() throws Exception {
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        List<RoaringBitmap> ands = Lists.newArrayList();
        int numBits = 10;
        int andBits = 3;
        for (int i = 0; i < numBits - andBits; i++) {
            TIntArrayList bits = new TIntArrayList();
            for (int j = i + 1; j < numBits; j++) {
                bits.add(j * 137);
            }
            ands.add(bitmaps.createWithBits(bits.toArray()));
        }
        RoaringBitmap container = bitmaps.and(ands);
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
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
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
        RoaringBitmap original = bitmaps.createWithBits(originalBits.toArray());
        RoaringBitmap not = bitmaps.createWithBits(notBits.toArray());
        RoaringBitmap container = bitmaps.andNot(original, not);
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
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        List<RoaringBitmap> nots = Lists.newArrayList();
        int numOriginal = 10;
        int numNot = 3;
        TIntArrayList originalBits = new TIntArrayList();
        for (int i = 0; i < numOriginal; i++) {
            originalBits.add(i * 137);
            if (i < numNot) {
                RoaringBitmap not = bitmaps.createWithBits(i * 137);
                nots.add(not);
            }
        }
        RoaringBitmap original = bitmaps.createWithBits(originalBits.toArray());
        RoaringBitmap container = bitmaps.andNot(original, nots);
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
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
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
        RoaringBitmap original = bitmaps.createWithBits(originalBits.toArray());
        RoaringBitmap not = bitmaps.createWithBits(notBits.toArray());
        CardinalityAndLastSetBit<RoaringBitmap> cardinalityAndLastSetBit = bitmaps.andNotWithCardinalityAndLastSetBit(original, not);
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
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();
        List<RoaringBitmap> ands = Lists.newArrayList();
        int numOriginal = 10;
        int numAnd = 3;
        for (int i = 0; i < numOriginal - numAnd; i++) {
            TIntArrayList andBits = new TIntArrayList();
            for (int j = i + 1; j < numOriginal; j++) {
                andBits.add(j * 137);
            }
            ands.add(bitmaps.createWithBits(andBits.toArray()));
        }
        CardinalityAndLastSetBit<RoaringBitmap> cardinalityAndLastSetBit = bitmaps.andWithCardinalityAndLastSetBit(ands);
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

    @Test
    public void testMultiAndNotWithCounts() throws Exception {
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();

        RoaringBitmap original = RoaringBitmap.bitmapOf(0, 1, 2, 3, 4, 5, 6, 7);
        long[] counts = new long[4];
        bitmaps.inPlaceAndNotMultiTx(original, (tx, stackBuffer) -> {
            tx.tx(0, 1, RoaringBitmap.bitmapOf(0, 1), null, -1, stackBuffer);
            tx.tx(1, 3, RoaringBitmap.bitmapOf(2, 3), null, -1, stackBuffer);
            tx.tx(2, 5, RoaringBitmap.bitmapOf(4, 5), null, -1, stackBuffer);
            tx.tx(3, 7, RoaringBitmap.bitmapOf(6, 7), null, -1, stackBuffer);
        }, counts, Optional.absent(), new StackBuffer());

        assertEquals(original.getCardinality(), 0);
        for (int i = 0; i < counts.length; i++) {
            assertEquals(counts[i], 2);
        }

        original = RoaringBitmap.bitmapOf(0, 1, 2, 3, 4, 5, 6, 7);
        RoaringBitmap counter = RoaringBitmap.bitmapOf(1, 3, 5, 7);
        counts = new long[4];
        bitmaps.inPlaceAndNotMultiTx(original, (tx, stackBuffer) -> {
            tx.tx(0, 1, RoaringBitmap.bitmapOf(0, 1), null, -1, stackBuffer);
            tx.tx(1, 3, RoaringBitmap.bitmapOf(2, 3), null, -1, stackBuffer);
            tx.tx(2, 5, RoaringBitmap.bitmapOf(4, 5), null, -1, stackBuffer);
            tx.tx(3, 7, RoaringBitmap.bitmapOf(6, 7), null, -1, stackBuffer);
        }, counts, Optional.of(counter), new StackBuffer());

        assertEquals(original.getCardinality(), 0);
        assertEquals(counter.getCardinality(), 0);
        for (int i = 0; i < counts.length; i++) {
            assertEquals(counts[i], 1);
        }
    }
}
