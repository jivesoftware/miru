package com.jivesoftware.os.miru.service.index.disk;

import com.jivesoftware.os.miru.service.index.MiruRemovalIndex;
import com.jivesoftware.os.jive.utils.keyed.store.SwappableFiler;

public class MiruOnDiskRemovalIndex extends MiruOnDiskInvertedIndex implements MiruRemovalIndex {

    public MiruOnDiskRemovalIndex(SwappableFiler swappableFiler) {
        super(swappableFiler);
    }

}
