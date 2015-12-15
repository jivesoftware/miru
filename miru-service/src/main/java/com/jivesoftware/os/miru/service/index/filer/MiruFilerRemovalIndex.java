package com.jivesoftware.os.miru.service.index.filer;

import com.jivesoftware.os.filer.io.api.KeyedFilerStore;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;
import com.jivesoftware.os.miru.plugin.partition.TrackError;

public class MiruFilerRemovalIndex<BM extends IBM, IBM> extends MiruFilerInvertedIndex<BM, IBM> implements MiruRemovalIndex<BM, IBM> {

    public MiruFilerRemovalIndex(MiruBitmaps<BM, IBM> bitmaps,
        TrackError trackError,
        KeyedFilerStore<Long, Void> keyedFilerStore,
        byte[] keyBytes,
        Object mutationLock) {

        super(bitmaps, trackError, -4, keyBytes, keyedFilerStore, mutationLock);
    }
}
