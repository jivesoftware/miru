package com.jivesoftware.os.miru.service.index.memory;

import com.googlecode.javaewah.EWAHCompressedBitmap;
import com.jivesoftware.os.miru.service.index.MiruRemovalIndex;

public class MiruInMemoryRemovalIndex extends MiruInMemoryInvertedIndex implements MiruRemovalIndex {

    public MiruInMemoryRemovalIndex(EWAHCompressedBitmap invertedIndex) {
        super(invertedIndex);
    }

}
