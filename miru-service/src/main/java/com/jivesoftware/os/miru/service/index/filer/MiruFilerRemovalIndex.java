package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;
import com.jivesoftware.os.miru.plugin.partition.TrackError;

public class MiruFilerRemovalIndex<BM extends IBM, IBM> extends MiruFilerInvertedIndex<BM, IBM> implements MiruRemovalIndex<IBM> {

    public MiruFilerRemovalIndex(MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        KeyedFilerStore<Long, Void> keyedFilerStore,
        byte[] keyBytes,
        int considerIfIndexIdGreaterThanN,
        Object mutationLock) {

        super(bitmaps, trackError, keyBytes, keyedFilerStore, considerIfIndexIdGreaterThanN, mutationLock);
    }
}
