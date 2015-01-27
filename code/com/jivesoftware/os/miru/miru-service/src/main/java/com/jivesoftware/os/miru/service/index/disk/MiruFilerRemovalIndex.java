package com.jivesoftware.os.miru.service.index.disk;

import com.jivesoftware.os.filer.map.store.api.KeyedFilerStore;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;

public class MiruFilerRemovalIndex<BM> extends MiruFilerInvertedIndex<BM> implements MiruRemovalIndex<BM> {

    public MiruFilerRemovalIndex(MiruBitmaps<BM> bitmaps,
        KeyedFilerStore keyedFilerStore,
        byte[] keyBytes,
        int considerIfIndexIdGreaterThanN,
        Object mutationLock) {

        super(bitmaps, keyedFilerStore, keyBytes, considerIfIndexIdGreaterThanN, mutationLock);
    }
}
