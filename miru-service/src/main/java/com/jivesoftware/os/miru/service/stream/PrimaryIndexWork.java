package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.miru.api.base.MiruTermId;
import gnu.trove.list.TIntList;
import gnu.trove.list.TLongList;

/**
 *
 */
class PrimaryIndexWork implements Comparable<PrimaryIndexWork> {
    final MiruTermId fieldValue;
    final TIntList ids;
    final TLongList counts;

    PrimaryIndexWork(MiruTermId fieldValue, TIntList ids, TLongList counts) {
        this.fieldValue = fieldValue;
        this.ids = ids;
        this.counts = counts;
    }

    @Override
    public int compareTo(PrimaryIndexWork o) {
        // flipped so that natural ordering is "largest first"
        return Integer.compare(o.ids.size(), ids.size());
    }
}
