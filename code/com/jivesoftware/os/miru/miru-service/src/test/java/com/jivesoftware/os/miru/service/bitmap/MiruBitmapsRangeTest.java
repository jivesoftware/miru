package com.jivesoftware.os.miru.service.bitmap;

import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.bitmap.MiruIntIterator;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 *
 */
public class MiruBitmapsRangeTest {

    @Test(dataProvider = "bitmapsProvider")
    public <BM> void testRanges(MiruBitmaps<BM> bitmaps) {
        int[] indexes = { 3, 4, 5, 8, 9, 10, 13, 15, 17, 18, 19, 20, 105 };
        BM bitmap = bitmaps.createWithBits(indexes);
        MiruIntIterator intIterator = bitmaps.intIterator(bitmap);

        assertEquals(bitmaps.cardinality(bitmap), indexes.length);
        assertEquals(bitmaps.lastSetBit(bitmap), indexes[indexes.length - 1]);

        int i = 0;
        while (intIterator.hasNext()) {
            assertEquals(intIterator.next(), indexes[i]);
            i++;
        }
    }

    @DataProvider(name = "bitmapsProvider")
    public Object[][] bitmapsProvider() {
        return new Object[][] {
            { new MiruBitmapsRoaring() },
            { new MiruBitmapsEWAH(16) }
        };
    }
}
