package com.jivesoftware.os.miru.cluster.rcvs;

/**
*
*/
public enum MiruHostsColumnKey {

    // explicit index is safer than ordinal and marshalls shorter than name
    //TODO consider compacting index to a byte
    heartbeat(0);

    private final static MiruHostsColumnKey[] states;

    static {
        MiruHostsColumnKey[] values = values();
        int maxIndex = -1;
        for (MiruHostsColumnKey value : values) {
            maxIndex = Math.max(maxIndex, value.index);
        }
        states = new MiruHostsColumnKey[maxIndex + 1];
        for (MiruHostsColumnKey value : values) {
            states[value.index] = value;
        }
    }

    private final int index;

    private MiruHostsColumnKey(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    public static MiruHostsColumnKey fromIndex(int index) {
        return states[index];
    }
}
