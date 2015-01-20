package com.jivesoftware.os.miru.api;

/**
 *
 */
public enum MiruBackingStorage {

    hybrid(true, false, 5, 0, 4, 6),
    mem_mapped(false, true, 1, 2),
    unknown(false, false, 3);

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
    private final boolean memoryBacked;
    private final boolean diskBacked;

    private MiruBackingStorage(boolean memoryBacked, boolean diskBacked, int... index) {
        this.memoryBacked = memoryBacked;
        this.diskBacked = diskBacked;
        this.index = index;
    }

    public int getIndex() {
        return index[0];
    }

    public static MiruBackingStorage fromIndex(int index) {
        return storages[index];
    }

    public boolean isDiskBacked() {
        return diskBacked;
    }

    public boolean isMemoryBacked() {
        return memoryBacked;
    }

}
