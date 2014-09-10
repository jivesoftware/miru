package org.roaringbitmap;

import com.jivesoftware.os.miru.query.CardinalityAndLastSetBit;
import com.jivesoftware.os.miru.service.bitmap.MiruBitmapsRoaring;
import java.util.Arrays;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class RoaringInspectionTest {

    @Test
    public void testCardinalityAndLastSetBit() throws Exception {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i * 37 < 5 * Short.MAX_VALUE; i++) {
            bitmap.add(i * 37);
            CardinalityAndLastSetBit cardinalityAndLastSetBit = RoaringInspection.cardinalityAndLastSetBit(bitmap);
            assertEquals(cardinalityAndLastSetBit.cardinality, i + 1);
            assertEquals(cardinalityAndLastSetBit.lastSetBit, i * 37);
        }
    }

    @Test
    public void testBoundary() throws Exception {
        MiruBitmapsRoaring bitmaps = new MiruBitmapsRoaring();

        RoaringBitmap bitmap = bitmaps.create();
        bitmaps.set(bitmap, 0);
        CardinalityAndLastSetBit cardinalityAndLastSetBit = RoaringInspection.cardinalityAndLastSetBit(bitmap);

        System.out.println("cardinalityAndLastSetBit="+cardinalityAndLastSetBit.lastSetBit);

        RoaringBitmap remove = bitmaps.create();
        bitmaps.set(remove, 0);

        RoaringBitmap answer = bitmaps.create();
        bitmaps.andNot(answer, bitmap, Arrays.asList(remove));

        cardinalityAndLastSetBit = RoaringInspection.cardinalityAndLastSetBit(answer);
        System.out.println("cardinalityAndLastSetBit="+cardinalityAndLastSetBit.lastSetBit);

    }

    @Test
    public void testSizeInBits() throws Exception {
        RoaringBitmap bitmap = new RoaringBitmap();

        assertEquals(RoaringInspection.sizeInBits(bitmap), 0);
        bitmap.add(0);
        assertEquals(RoaringInspection.sizeInBits(bitmap), 1 << 16);
        bitmap.add(1 << 16 - 1);
        assertEquals(RoaringInspection.sizeInBits(bitmap), 1 << 16);
        bitmap.add(1 << 16);
        assertEquals(RoaringInspection.sizeInBits(bitmap), 2 << 16);
        bitmap.add(2 << 16 - 1);
        assertEquals(RoaringInspection.sizeInBits(bitmap), 2 << 16);
        bitmap.add(2 << 16);
        assertEquals(RoaringInspection.sizeInBits(bitmap), 3 << 16);
    }
}