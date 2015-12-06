package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.miru.api.base.MiruTermId;

/**
 *
 */
public interface IndexAlignedBitmapStream<BM> {

    void stream(int index, BM bitmap) throws Exception;
}
