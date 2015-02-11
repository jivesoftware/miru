package com.jivesoftware.os.miru.service.index.filer;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.jivesoftware.os.filer.map.store.api.KeyedFilerStore;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;

public class MiruFilerRemovalIndex<BM> extends MiruFilerInvertedIndex<BM> implements MiruRemovalIndex<BM> {

    public MiruFilerRemovalIndex(MiruBitmaps<BM> bitmaps,
        Cache<MiruFieldIndex.IndexKey, Optional<?>> fieldIndexCache,
        long indexId,
        KeyedFilerStore keyedFilerStore,
        byte[] keyBytes,
        int considerIfIndexIdGreaterThanN,
        Object mutationLock) {

        super(bitmaps, fieldIndexCache, new MiruFieldIndex.IndexKey(indexId, keyBytes), keyedFilerStore, considerIfIndexIdGreaterThanN, mutationLock);
    }
}
