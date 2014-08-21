package com.jivesoftware.os.miru.service.index.memory;

import com.jivesoftware.os.miru.service.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.service.index.MiruRemovalIndex;

public class MiruInMemoryRemovalIndex<BM> extends MiruInMemoryInvertedIndex<BM> implements MiruRemovalIndex<BM> {

    public MiruInMemoryRemovalIndex(MiruBitmaps<BM> bitmaps) {
        super(bitmaps);
    }

}
