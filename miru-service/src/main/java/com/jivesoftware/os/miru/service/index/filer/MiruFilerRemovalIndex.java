package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;

public class MiruFilerRemovalIndex<BM extends IBM, IBM> extends MiruFilerInvertedIndex<BM, IBM> implements MiruRemovalIndex<IBM> {

    public MiruFilerRemovalIndex(MiruBitmaps<BM, IBM> bitmaps,
        KeyedFilerStore<Long, Void> keyedFilerStore,
        byte[] keyBytes,
        int considerIfIndexIdGreaterThanN,
        Object mutationLock) {

        super(bitmaps, keyBytes, keyedFilerStore, considerIfIndexIdGreaterThanN, mutationLock);
    }
}
