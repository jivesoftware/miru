package com.jivesoftware.os.miru.api.field;

/**
 *
 */
public enum MiruFieldType {

    // -3 reserved for authz
    // -2 reserved for inbox
    // -1 reserved for unread
    primary(0),
    latest(1),
    pairedLatest(2),
    bloom(3),
    valueBits(4);

    private int index;

    MiruFieldType(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }
}
