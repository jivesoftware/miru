package com.jivesoftware.os.miru.service.stream;

import com.jivesoftware.os.miru.api.activity.schema.MiruFieldDefinition;
import gnu.trove.list.TIntList;

/**
 *
 */
class ValueIndexWork implements Comparable<ValueIndexWork> {
    final MiruFieldDefinition fieldDefinition; // parent
    final TIntList allIds; // 99, 100, 101, 102, 103
    final TIntList setIds; // 99, 101, 103
    final int bit; // 0

    public ValueIndexWork(MiruFieldDefinition fieldDefinition, TIntList allIds, TIntList setIds, int bit) {
        this.fieldDefinition = fieldDefinition;
        this.allIds = allIds;
        this.setIds = setIds;
        this.bit = bit;
    }

    @Override
    public int compareTo(ValueIndexWork o) {
        int c = Integer.compare(setIds.size(), o.setIds.size());
        if (c == 0) {
            c = Integer.compare(bit, o.bit);
        }
        if (c == 0) {
            c = Integer.compare(fieldDefinition.fieldId, o.fieldDefinition.fieldId);
        }
        return c;
    }
}
