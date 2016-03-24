package com.jivesoftware.os.miru.service.stream;

import gnu.trove.list.TIntList;

/**
 *
 */
class ValueIndexWork implements Comparable<ValueIndexWork> {
    final TIntList allIds; // 99, 100, 101, 102, 103
    final TIntList setIds; // 99, 101, 103
    final int bit; // 0

    public ValueIndexWork(TIntList allIds, TIntList setIds, int bit) {
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
        return c;
    }
}
