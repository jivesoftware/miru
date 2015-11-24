package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.miru.api.base.MiruTermId;

/**
 *
 */
public interface TermIdStream {

    boolean stream(MiruTermId termId) throws Exception;
}
