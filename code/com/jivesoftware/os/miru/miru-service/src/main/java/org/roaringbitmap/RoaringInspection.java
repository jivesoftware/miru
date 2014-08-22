package org.roaringbitmap;

import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps;

/**
 *
 */
public class RoaringInspection {

    public static MiruBitmaps.CardinalityAndLastSetBit cardinalityAndLastSetBit(RoaringBitmap bitmap) {
        int pos = bitmap.highLowContainer.size() - 1;
        int lastSetBit = -1;
        while (pos >= 0) {
            Container lastContainer = bitmap.highLowContainer.array[pos].value;
            ShortIterator shortIterator = lastContainer.getShortIterator();
            short last = -1;
            while (shortIterator.hasNext()) {
                last = shortIterator.next();
            }
            if (last >= 0) {
                int hs = Util.toIntUnsigned(bitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
                lastSetBit = Util.toIntUnsigned(last) | hs;
                break;
            }
            pos--;
        }
        return new MiruBitmaps.CardinalityAndLastSetBit(bitmap.getCardinality(), lastSetBit);
    }
}
