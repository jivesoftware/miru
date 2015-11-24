package com.jivesoftware.os.miru.service.index.delta;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruInvertedIndex;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;

public class MiruDeltaRemovalIndex<BM extends IBM, IBM> extends MiruDeltaInvertedIndex<BM, IBM> implements MiruRemovalIndex<IBM> {

    public MiruDeltaRemovalIndex(MiruBitmaps<BM, IBM> bitmaps,
        Cache<MiruFieldIndex.IndexKey, Optional<?>> fieldIndexCache,
        Cache<MiruFieldIndex.IndexKey, Long> versionCache,
        long indexId,
        byte[] keyBytes,
        MiruInvertedIndex<IBM> backingIndex,
        Delta<IBM> delta) {
        super(bitmaps, backingIndex, delta, new MiruFieldIndex.IndexKey(indexId, keyBytes), fieldIndexCache, versionCache);
    }
}
