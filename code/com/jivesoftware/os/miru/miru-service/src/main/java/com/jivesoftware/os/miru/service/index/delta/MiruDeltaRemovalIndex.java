package com.jivesoftware.os.miru.service.index.delta;

import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;

public class MiruDeltaRemovalIndex<BM> extends MiruDeltaInvertedIndex<BM> implements MiruRemovalIndex<BM> {

    public MiruDeltaRemovalIndex(MiruBitmaps<BM> bitmaps,
        MiruInvertedIndex<BM> backingIndex,
        Delta<BM> delta) {
        super(bitmaps, backingIndex, delta);
    }
}
