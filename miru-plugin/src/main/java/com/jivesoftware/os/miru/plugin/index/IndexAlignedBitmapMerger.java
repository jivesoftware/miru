package com.jivesoftware.os.miru.plugin.index;

import java.io.IOException;

/**
 *
 */
public interface IndexAlignedBitmapMerger<BM> {
    BitmapAndLastId<BM> merge(int index, BitmapAndLastId<BM> backing) throws IOException;
}
