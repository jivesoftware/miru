package com.jivesoftware.os.miru.service.index.disk;

import com.jivesoftware.os.jive.utils.keyed.store.SwappableFiler;
import com.jivesoftware.os.miru.service.index.MiruRemovalIndex;

public class MiruOnDiskRemovalIndex extends MiruOnDiskInvertedIndex implements MiruRemovalIndex {

    public MiruOnDiskRemovalIndex(SwappableFiler swappableFiler) {
        super(swappableFiler);
    }

}
