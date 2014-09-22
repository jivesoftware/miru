package com.jivesoftware.os.miru.cluster.rcvs;

/**
*
*/
public enum MiruSchemaColumnKey {

    // explicit index is safer than ordinal and marshalls shorter than name
    //TODO consider compacting index to a byte
    schema(0);

    private final static MiruSchemaColumnKey[] states;

    static {
        MiruSchemaColumnKey[] values = values();
        int maxIndex = -1;
        for (MiruSchemaColumnKey value : values) {
            maxIndex = Math.max(maxIndex, value.index);
        }
        states = new MiruSchemaColumnKey[maxIndex + 1];
        for (MiruSchemaColumnKey value : values) {
            states[value.index] = value;
        }
    }

    private final int index;

    private MiruSchemaColumnKey(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    public static MiruSchemaColumnKey fromIndex(int index) {
        return states[index];
    }
}
