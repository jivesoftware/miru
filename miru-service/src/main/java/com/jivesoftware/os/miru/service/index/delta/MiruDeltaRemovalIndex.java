package com.jivesoftware.os.miru.service.index.delta;

import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;
import com.jivesoftware.os.miru.plugin.partition.TrackError;

public class MiruDeltaRemovalIndex<BM extends IBM, IBM> extends MiruDeltaInvertedIndex<BM, IBM> implements MiruRemovalIndex<IBM> {

    public MiruDeltaRemovalIndex(MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        MiruInvertedIndex<IBM> backingIndex,
        Delta<IBM> delta) {
        super(bitmaps, trackError, backingIndex, delta);
    }
}
