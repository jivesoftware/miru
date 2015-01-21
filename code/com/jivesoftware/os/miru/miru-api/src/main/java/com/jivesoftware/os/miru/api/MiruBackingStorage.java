package com.jivesoftware.os.miru.api;

/**
 *
 */
public enum MiruBackingStorage {

    // includes legacy ordinals
    memory(5, 0, 4, 6),
    disk(1, 2),
    unknown(3);

    private final static MiruBackingStorage[] storages;

    static {
        storages = new MiruBackingStorage[7];
        for (MiruBackingStorage value : values()) {
            for (int i : value.index) {
                storages[i] = value;
            }
        }
    }

    private final int[] index;

    private MiruBackingStorage(int... index) {
        this.index = index;
    }

    public int getIndex() {
        return index[0];
    }

    public static MiruBackingStorage fromIndex(int index) {
        return storages[index];
    }

}
