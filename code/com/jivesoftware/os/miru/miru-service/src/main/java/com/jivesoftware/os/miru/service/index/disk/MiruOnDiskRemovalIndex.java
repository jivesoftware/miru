package com.jivesoftware.os.miru.service.index.disk;

import com.jivesoftware.os.filer.keyed.store.KeyedFilerStore;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;

public class MiruOnDiskRemovalIndex<BM> extends MiruOnDiskInvertedIndex<BM> implements MiruRemovalIndex<BM> {

    public MiruOnDiskRemovalIndex(MiruBitmaps<BM> bitmaps,
        KeyedFilerStore keyedFilerStore,
        byte[] keyBytes,
        int considerIfIndexIdGreaterThanN,
        long initialCapacityInBytes,
        Object mutationLock) {

        super(bitmaps, keyedFilerStore, keyBytes, considerIfIndexIdGreaterThanN, initialCapacityInBytes, mutationLock);
    }
}
