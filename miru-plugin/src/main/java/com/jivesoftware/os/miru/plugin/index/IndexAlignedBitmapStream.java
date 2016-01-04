package com.jivesoftware.os.miru.plugin.index;

/**
 *
 */
public interface IndexAlignedBitmapStream<BM> {

    void stream(int index, BM bitmap) throws Exception;
}
