package org.roaringbitmap;

import com.jivesoftware.os.miru.query.CardinalityAndLastSetBit;

/**
 *
 */
public class RoaringInspection {

    public static CardinalityAndLastSetBit cardinalityAndLastSetBit(RoaringBitmap bitmap) {
        int pos = bitmap.highLowContainer.size() - 1;
        int lastSetBit = -1;
        while (pos >= 0) {
            Container lastContainer = bitmap.highLowContainer.array[pos].value;
            lastSetBit = lastSetBit(bitmap, lastContainer, pos);
            if (lastSetBit >= 0) {
                break;
            }
            pos--;
        }
        int cardinality = bitmap.getCardinality();
        assert cardinality == 0 || lastSetBit >= 0;
        return new CardinalityAndLastSetBit(cardinality, lastSetBit);
    }

    private static int lastSetBit(RoaringBitmap bitmap, Container container, int pos) {
        if (container instanceof ArrayContainer) {
            ArrayContainer arrayContainer = (ArrayContainer) container;
            int cardinality = arrayContainer.cardinality;
            if (cardinality > 0) {
                int hs = Util.toIntUnsigned(bitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
                short last = arrayContainer.content[cardinality - 1];
                return Util.toIntUnsigned(last) | hs;
            }
        } else {
            // <-- trailing              leading -->
            // [ 0, 0, 0, 0, 0 ... , 0, 0, 0, 0, 0 ]
            BitmapContainer bitmapContainer = (BitmapContainer) container;
            long[] longs = bitmapContainer.bitmap;
            for (int i = longs.length - 1; i >= 0; i--) {
                long l = longs[i];
                int leadingZeros = Long.numberOfLeadingZeros(l);
                if (leadingZeros < 64) {
                    int hs = Util.toIntUnsigned(bitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
                    short last = (short) ((i * 64) + 64 - leadingZeros - 1);
                    return Util.toIntUnsigned(last) | hs;
                }
            }
        }
        return -1;
    }

    public static long sizeInBits(RoaringBitmap bitmap) {
        int pos = bitmap.highLowContainer.size() - 1;
        int sizeInBits = -1;
        if (pos >= 0) {
            int hs = Util.toIntUnsigned(bitmap.highLowContainer.getKeyAtIndex(pos)) << 16;
            Container container = bitmap.highLowContainer.array[pos].value;
            if (container instanceof ArrayContainer) {
                ArrayContainer arrayContainer = (ArrayContainer) container;
                int cardinality = arrayContainer.cardinality;
                short last = (cardinality > 0) ? arrayContainer.content[cardinality - 1] : 0;
                return Util.toIntUnsigned(last) | hs;
            } else {
                // <-- trailing              leading -->
                // [ 0, 0, 0, 0, 0 ... , 0, 0, 0, 0, 0 ]
                BitmapContainer bitmapContainer = (BitmapContainer) container;
                int i = bitmapContainer.bitmap.length - 1;
                short last = (i >= 0) ? (short) (i * 64 - 1) : 0;
                return Util.toIntUnsigned(last) | hs;
            }
        }
        return sizeInBits;
    }
}
