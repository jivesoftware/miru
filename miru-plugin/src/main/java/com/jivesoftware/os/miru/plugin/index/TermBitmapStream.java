package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.miru.api.base.MiruTermId;

/**
 *
 */
public interface TermBitmapStream<BM> {

    boolean stream(MiruTermId termId, BM bitmap) throws Exception;
}
