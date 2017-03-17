package com.jivesoftware.os.miru.sync.api;

/**
 * Created by jonathan.colt on 12/22/16.
 */
public enum MiruSyncTimeShiftStrategy {
    none(0),
    linear(1);

    private final int id;

    MiruSyncTimeShiftStrategy(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static MiruSyncTimeShiftStrategy fromId(int id) {
        for (MiruSyncTimeShiftStrategy strategy : values()) {
            if (strategy.id == id) {
                return strategy;
            }
        }
        return null;
    }
}
