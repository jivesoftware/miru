package com.jivesoftware.os.miru.service.index.memory;


import com.jivesoftware.os.miru.query.MiruBitmaps;
import com.jivesoftware.os.miru.query.MiruRemovalIndex;

public class MiruInMemoryRemovalIndex<BM> extends MiruInMemoryInvertedIndex<BM> implements MiruRemovalIndex<BM> {

    public MiruInMemoryRemovalIndex(MiruBitmaps<BM> bitmaps) {
        super(bitmaps);
    }

}
