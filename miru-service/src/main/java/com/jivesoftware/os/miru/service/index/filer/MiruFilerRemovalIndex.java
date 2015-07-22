package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruFieldIndex;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;

public class MiruFilerRemovalIndex<BM> extends MiruFilerInvertedIndex<BM> implements MiruRemovalIndex<BM> {

    public MiruFilerRemovalIndex(MiruBitmaps<BM> bitmaps,
        long indexId,
        KeyedFilerStore<Long, Void> keyedFilerStore,
        byte[] keyBytes,
        int considerIfIndexIdGreaterThanN,
        Object mutationLock) {

        super(bitmaps, new MiruFieldIndex.IndexKey(indexId, keyBytes), keyedFilerStore, considerIfIndexIdGreaterThanN, mutationLock);
    }
}
