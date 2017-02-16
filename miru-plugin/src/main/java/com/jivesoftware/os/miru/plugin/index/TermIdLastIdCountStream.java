package com.jivesoftware.os.miru.plugin.index;

import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.plugin.solution.TermIdLastIdCount;

/**
 *
 */
public interface TermIdLastIdCountStream {

    boolean stream(TermIdLastIdCount termIdLastIdCount) throws Exception;
}
