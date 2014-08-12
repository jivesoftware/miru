package com.jivesoftware.os.miru.service.query.base;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.googlecode.javaewah.IntIterator;

/**
 *
 * @author jonathan
 */
public class EWAHUtils {

    public int lastSetBit(EWAHCompressedBitmap bitmap) {
        IntIterator iterator = bitmap.intIterator();
        int last = -1;
        for (; iterator.hasNext();) {
            last = iterator.next();
        }
        return last;
    }
}
