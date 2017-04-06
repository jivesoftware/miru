package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.miru.api.base.MiruTermId;

/**
 *
 */
public interface IdAndTermIdStream {

    boolean stream(int id, MiruTermId termId, long count) throws Exception;
}
