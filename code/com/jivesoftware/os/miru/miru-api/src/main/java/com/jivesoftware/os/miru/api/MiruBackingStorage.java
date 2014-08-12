package com.jivesoftware.os.miru.api;

import com.google.common.collect.ImmutableBiMap;
import java.util.Map;

/**
 *
 */
public enum MiruBackingStorage {

    // explicit index is safer than ordinal and marshalls shorter than name
    //TODO consider compacting index to a byte
    memory(0, true, false, false),
    memory_fixed(4, true, false, true),
    hybrid(5, true, false, false),
    hybrid_fixed(6, true, false, true),
    mem_mapped(1, false, true, false),
    disk(2, false, true, false),
    unknown(3, false, false, false);

    private final static MiruBackingStorage[] storages;
    private final static Map<MiruBackingStorage, MiruBackingStorage> identical;

    static {
        MiruBackingStorage[] values = values();
        int maxIndex = -1;
        for (MiruBackingStorage value : values) {
            maxIndex = Math.max(maxIndex, value.index);
        }
        storages = new MiruBackingStorage[maxIndex + 1];
        for (MiruBackingStorage value : values) {
            storages[value.index] = value;
        }

        identical = ImmutableBiMap.<MiruBackingStorage, MiruBackingStorage>builder()
            .put(memory, memory_fixed)
            .put(memory_fixed, memory)
            .put(hybrid, hybrid_fixed)
            .put(hybrid_fixed, hybrid)
            .put(mem_mapped, disk)
            .put(disk, mem_mapped)
            .build();
    }

    private final int index;
    private final boolean memoryBacked;
    private final boolean diskBacked;
    private final boolean fixed;

    private MiruBackingStorage(int index, boolean memoryBacked, boolean diskBacked, boolean fixed) {
        this.index = index;
        this.memoryBacked = memoryBacked;
        this.diskBacked = diskBacked;
        this.fixed = fixed;
    }

    public int getIndex() {
        return index;
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

    public boolean isFixed() {
        return fixed;
    }

    public boolean isIdentical(MiruBackingStorage storage) {
        return identical.get(this) == storage;
    }
}
