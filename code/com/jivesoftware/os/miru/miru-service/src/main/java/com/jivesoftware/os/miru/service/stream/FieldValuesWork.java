package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.miru.api.base.MiruTermId;
import gnu.trove.list.TIntList;

/**
 *
 */
class FieldValuesWork implements Comparable<FieldValuesWork> {
    final MiruTermId fieldValue;
    final TIntList ids;

    FieldValuesWork(MiruTermId fieldValue, TIntList ids) {
        this.fieldValue = fieldValue;
        this.ids = ids;
    }

    @Override
    public int compareTo(FieldValuesWork o) {
        // flipped so that natural ordering is "largest first"
        return Integer.compare(o.ids.size(), ids.size());
    }
}
