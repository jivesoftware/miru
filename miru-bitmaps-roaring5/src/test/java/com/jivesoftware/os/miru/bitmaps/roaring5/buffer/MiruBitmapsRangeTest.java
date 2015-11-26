package com.jivesoftware.os.miru.bitmaps.roaring5.buffer;

import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class MiruBitmapsRangeTest {

    @Test
    public void testRanges() {
        MiruBitmapsRoaringBuffer bitmaps = new MiruBitmapsRoaringBuffer();
        int[] indexes = { 3, 4, 5, 8, 9, 10, 13, 15, 17, 18, 19, 20, 105 };
        MutableRoaringBitmap bitmap = bitmaps.createWithBits(indexes);
        MiruIntIterator intIterator = bitmaps.intIterator(bitmap);

        assertEquals(bitmaps.cardinality(bitmap), indexes.length);
        assertEquals(bitmaps.lastSetBit(bitmap), indexes[indexes.length - 1]);

        int i = 0;
        while (intIterator.hasNext()) {
            assertEquals(intIterator.next(), indexes[i]);
            i++;
        }
    }
}
