package com.jivesoftware.os.miru.plugin.index;

/**
 *
 */
public class BitmapAndLastId<BM> {
    private BM bitmap;
    private int lastId;

    public BitmapAndLastId() {
    }

    public BitmapAndLastId<BM> set(BM bitmap, int lastId) {
        this.bitmap = bitmap;
        this.lastId = lastId;
        return this;
    }

    public void clear() {
        this.bitmap = null;
        this.lastId = -1;
    }

    public BM getBitmap() {
        return bitmap;
    }

    public int getLastId() {
        return lastId;
    }

    public boolean isSet() {
        return (bitmap != null);
    }

    @Override
    public String toString() {
        return "BitmapAndLastId{" +
            "bitmap=" + bitmap +
            ", lastId=" + lastId +
            '}';
    }
}
