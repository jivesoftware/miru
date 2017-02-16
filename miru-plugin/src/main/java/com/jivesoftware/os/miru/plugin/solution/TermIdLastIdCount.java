package com.jivesoftware.os.miru.plugin.solution;

import com.jivesoftware.os.miru.api.base.MiruTermId;

/**
 *
 */
public class TermIdLastIdCount {

    public final MiruTermId termId;
    public int lastId;
    public long count;

    public TermIdLastIdCount(MiruTermId termId, int lastId, long count) {
        this.lastId = lastId;
        this.termId = termId;
        this.count = count;
    }
}
