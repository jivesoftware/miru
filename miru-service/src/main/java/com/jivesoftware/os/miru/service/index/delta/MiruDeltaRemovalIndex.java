package com.jivesoftware.os.miru.service.index.delta;

import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;

public class MiruDeltaRemovalIndex<BM extends IBM, IBM> extends MiruDeltaInvertedIndex<BM, IBM> implements MiruRemovalIndex<IBM> {

    public MiruDeltaRemovalIndex(MiruBitmaps<BM, IBM> bitmaps,
        MiruInvertedIndex<IBM> backingIndex,
        Delta<IBM> delta) {
        super(bitmaps, backingIndex, delta);
    }
}
