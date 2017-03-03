package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.filer.io.api.StackBuffer;

public interface MiruInvertedIndexAppender {

    /**
     * Sets multiple bits anywhere in the bitmap.
     *
     * @param ids the indexes of the bits to set
     * @throws Exception
     */
    void set(StackBuffer stackBuffer, int... ids) throws Exception;

}
