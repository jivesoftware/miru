package com.jivesoftware.os.miru.service.index.delta;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;

public class MiruDeltaRemovalIndex<BM> extends MiruDeltaInvertedIndex<BM> implements MiruRemovalIndex<BM> {

    public MiruDeltaRemovalIndex(MiruBitmaps<BM> bitmaps,
        Cache<MiruFieldIndex.IndexKey, Optional<?>> fieldIndexCache,
        long indexId,
        byte[] keyBytes,
        MiruInvertedIndex<BM> backingIndex,
        Delta<BM> delta) {
        super(bitmaps, backingIndex, delta, new MiruFieldIndex.IndexKey(indexId, keyBytes), fieldIndexCache);
    }
}
