package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.miru.api.base.MiruTermId;
import gnu.trove.list.TIntList;
import gnu.trove.list.TLongList;

/**
 *
 */
class FieldValuesWork implements Comparable<FieldValuesWork> {
    final MiruTermId fieldValue;
    final TIntList ids;
    final TLongList counts;

    FieldValuesWork(MiruTermId fieldValue, TIntList ids, TLongList counts) {
        this.fieldValue = fieldValue;
        this.ids = ids;
        this.counts = counts;
    }

    @Override
    public int compareTo(FieldValuesWork o) {
        // flipped so that natural ordering is "largest first"
        return Integer.compare(o.ids.size(), ids.size());
    }
}
