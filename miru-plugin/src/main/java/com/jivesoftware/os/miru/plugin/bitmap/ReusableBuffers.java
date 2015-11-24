package com.jivesoftware.os.miru.plugin.bitmap;

/**
 * Cycles between buffers with the expectation that each buffer is derived from no more than "size - 1" previous buffers. For example, to aggregate a previous
 * reusable answer plus an additional reusable bitmap into a new answer, "size" must be at least 3. However, if the previous answer is reusable but the
 * additional bitmap is non-reusable, then "size" need only be 2.
 */
public class ReusableBuffers<BM extends IBM, IBM> {

    private int index = 0;
    private final MiruBitmaps<BM, IBM> bitmaps;
    private final BM[] bufs;

    public ReusableBuffers(MiruBitmaps<BM, IBM> bitmaps, int size) {
        this.bitmaps = bitmaps;
        this.bufs = null; // bitmaps.createArrayOf(size);
    }

    public BM next() {
        return bitmaps.create();
        /*
        BM buf = bufs[index++ % bufs.length];
        bitmaps.clear(buf);
        return buf;
        */
    }

    public void retain(BM keep, BM replaceWith) {
        /*
        for (int i = 0; i < bufs.length; i++) {
            if (bufs[i] == keep) {
                bufs[i] = replaceWith;
            }
        }
        */
    }
}
