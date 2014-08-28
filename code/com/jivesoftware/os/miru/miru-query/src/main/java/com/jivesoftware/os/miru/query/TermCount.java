package com.jivesoftware.os.miru.query;

import com.jivesoftware.os.miru.api.base.MiruTermId;

/**
*
*/
public class TermCount implements BloomIndex.HasValue {

    public final MiruTermId termId;
    public MiruTermId[] mostRecent;
    public final long count;

    public TermCount(MiruTermId termId, MiruTermId[] mostRecent, long count) {
        this.termId = termId;
        this.mostRecent = mostRecent;
        this.count = count;
    }

    @Override
    public byte[] getValue() {
        return termId.getBytes();
    }

    @Override
    public String toString() {
        return "TermCount{" + "termId=" + termId + ", mostRecent=" + mostRecent + ", count=" + count + '}';
    }

}
