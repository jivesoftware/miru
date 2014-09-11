package com.jivesoftware.os.miru.query.solution;

import com.jivesoftware.os.miru.api.base.MiruTermId;
import com.jivesoftware.os.miru.query.index.BloomIndex;

/**
*
*/
public class MiruTermCount implements BloomIndex.HasValue {

    public final MiruTermId termId;
    public MiruTermId[] mostRecent;
    public final long count;

    public MiruTermCount(MiruTermId termId, MiruTermId[] mostRecent, long count) {
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
