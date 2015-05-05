package com.jivesoftware.os.miru.api;

/**
 *
 */
public enum MiruPartitionState {

    // explicit index is safer than ordinal and marshalls shorter than name
    //TODO consider compacting index to a byte
    offline(0),
    bootstrap(3),
    rebuilding(1),
    online(2);

    private final static MiruPartitionState[] states;

    static {
        MiruPartitionState[] values = values();
        int maxIndex = -1;
        for (MiruPartitionState value : values) {
            maxIndex = Math.max(maxIndex, value.index);
        }
        states = new MiruPartitionState[maxIndex + 1];
        for (MiruPartitionState value : values) {
            states[value.index] = value;
        }
    }

    private final int index;

    private MiruPartitionState(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    public static MiruPartitionState fromIndex(int index) {
        return states[index];
    }
}
