package com.jivesoftware.os.miru.service.index;

import com.googlecode.javaewah.EWAHCompressedBitmap;

/**
 * Cycles between buffers with the expectation that each buffer is derived from no more than "size - 1" previous buffers. For example, to aggregate a
 * previous reusable answer plus an additional reusable bitmap into a new answer, "size" must be at least 3. However, if the previous answer is reusable but
 * the additional bitmap is non-reusable, then "size" need only be 2.
 */
public class ReusableBuffers {

    private int index = 0;
    private final EWAHCompressedBitmap[] bufs;

    public ReusableBuffers(int size) {
        this.bufs = new EWAHCompressedBitmap[size];
        for (int i = 0; i < size; i++) {
            bufs[i] = new EWAHCompressedBitmap();
        }
    }

    public EWAHCompressedBitmap next() {
        EWAHCompressedBitmap buf = bufs[index++ % bufs.length];
        buf.clear();
        return buf;
    }
}
