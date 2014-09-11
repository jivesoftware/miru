package com.jivesoftware.os.miru.service.index.disk;

import com.jivesoftware.os.jive.utils.keyed.store.SwappableFiler;
import com.jivesoftware.os.miru.query.bitmap.MiruBitmaps;
import com.jivesoftware.os.miru.query.index.MiruRemovalIndex;

public class MiruOnDiskRemovalIndex<BM> extends MiruOnDiskInvertedIndex<BM> implements MiruRemovalIndex<BM> {

    public MiruOnDiskRemovalIndex(MiruBitmaps<BM> bitmaps, SwappableFiler swappableFiler) {
        super(bitmaps, swappableFiler);
    }

}
