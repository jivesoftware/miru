package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.miru.api.base.MiruTermId;

/**
 *
 */
public interface LastIdAndTermIdStream {

    boolean stream(int lastId, MiruTermId termId) throws Exception;
}
