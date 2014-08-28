package org.roaringbitmap;

import com.jivesoftware.os.miru.query.CardinalityAndLastSetBit;
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
}