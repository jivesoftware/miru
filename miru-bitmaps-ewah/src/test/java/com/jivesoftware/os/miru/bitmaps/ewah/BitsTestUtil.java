package com.jivesoftware.os.miru.bitmaps.ewah;

import com.googlecode.javaewah.EWAHCompressedBitmap;

import static org.testng.Assert.assertTrue;

/**
 *
 */
public class BitsTestUtil {

    /**
     * Sets bits at intervals from the specified start index (inclusive), according to the given pattern, until
     * the end index (exclusive) is reached.
     * <p/>
     * For example, <code>ewahBits(5, 50, 2, 4, 8, 16)</code> would set bits [5, 7, 11, 19, 35, 37, 41, 49]
     *
     * @param start the start index (inclusive)
     * @param end the end index (exclusive)
     * @param pattern the pattern
     * @return the composed EWAH bitset
     */
    public static EWAHCompressedBitmap ewahBits(int start, int end, int... pattern) {
        assertTrue(start >= 0);
        assertTrue(start < end);

        EWAHCompressedBitmap bits = new EWAHCompressedBitmap();
        bits.set(start);
        int j = 0;
        int last = start;
        for (int i = start + 1; i < end; i++) {
            if (i == last + pattern[j % pattern.length]) {
                bits.set(i);
                last = i;
                j++;
            }
        }
        return bits;
    }

    private BitsTestUtil() {
    }
}
