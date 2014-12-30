package com.jivesoftware.os.miru.service.index.memory;


import com.jivesoftware.os.filer.map.store.api.KeyValueStore;
import com.jivesoftware.os.miru.plugin.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.plugin.index.MiruRemovalIndex;

public class MiruInMemoryRemovalIndex<BM> extends MiruInMemoryInvertedIndex<BM> implements MiruRemovalIndex<BM> {

    public MiruInMemoryRemovalIndex(MiruBitmaps<BM> bitmaps,
        KeyValueStore<byte[], ReadWrite<BM>> store,
        byte[] keyBytes,
        int considerIfIndexIdGreaterThanN) {
        super(bitmaps, store, keyBytes, considerIfIndexIdGreaterThanN);
    }
}
