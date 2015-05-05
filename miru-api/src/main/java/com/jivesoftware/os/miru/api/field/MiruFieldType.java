package com.jivesoftware.os.miru.api.field;

/**
 *
 */
public enum MiruFieldType {

    primary(0),
    latest(1),
    pairedLatest(2),
    bloom(3);

    private int index;

    MiruFieldType(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }
}
