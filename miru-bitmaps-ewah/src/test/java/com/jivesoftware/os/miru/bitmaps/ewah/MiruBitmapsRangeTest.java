package com.jivesoftware.os.miru.bitmaps.ewah;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class MiruBitmapsRangeTest {

    @Test
    public void testRanges() {
        MiruBitmapsEWAH bitmaps = new MiruBitmapsEWAH(16);

        int[] indexes = { 3, 4, 5, 8, 9, 10, 13, 15, 17, 18, 19, 20, 105 };
        EWAHCompressedBitmap bitmap = bitmaps.createWithBits(indexes);
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
