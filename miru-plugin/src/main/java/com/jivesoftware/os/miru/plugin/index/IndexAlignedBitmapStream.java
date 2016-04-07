package com.jivesoftware.os.miru.plugin.index;

/**
 *
 */
public interface IndexAlignedBitmapStream<BM> {

    void stream(int index, int lastId, BM bitmap) throws Exception;
}
