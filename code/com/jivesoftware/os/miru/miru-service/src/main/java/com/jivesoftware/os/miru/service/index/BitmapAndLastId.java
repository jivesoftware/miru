package com.jivesoftware.os.miru.service.index;

/**
 *
 */
public class BitmapAndLastId<BM> {
    public final BM bitmap;
    public final int lastId;

    public BitmapAndLastId(BM bitmap, int lastId) {
        this.bitmap = bitmap;
        this.lastId = lastId;
    }
}
