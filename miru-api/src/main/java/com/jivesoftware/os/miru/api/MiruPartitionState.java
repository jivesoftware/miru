package com.jivesoftware.os.miru.api;

/**
 *
 */
public enum MiruPartitionState {

    // explicit index is safer than ordinal and marshalls shorter than name
    //TODO consider compacting index to a byte
    offline(0, false, false, false),
    bootstrap(3, true, false, false),
    rebuilding(1, false, true, false),
    online(2, false, false, true),
    obsolete(4, true, false, true),
    upgrading(5, false, true, true); // means we are online and doing a rebuild because of a new schema

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
    private final boolean isRebuildable;
    private final boolean isRebuilding;
    private final boolean isOnline;

    private MiruPartitionState(int index, boolean isRebuildable, boolean isRebuilding, boolean isOnline) {
        this.index = index;
        this.isRebuildable = isRebuildable;
        this.isRebuilding = isRebuilding;
        this.isOnline = isOnline;
    }

    public MiruPartitionState transitionToRebuildingState() {
        if (isRebuildable()) {
            return this == obsolete ? upgrading : rebuilding;
        }
        throw new IllegalStateException(this + ". So punny. You cannot transition this state!");
    }

    public boolean isRebuildable() {
        return isRebuildable;
    }

    public boolean isRebuilding() {
        return isRebuilding;
    }

    public boolean isOnline() {
        return isOnline;
    }

    public int getIndex() {
        return index;
    }

    public static MiruPartitionState fromIndex(int index) {
        return states[index];
    }

    public static MiruPartitionState[] onlineStates() {
        return new MiruPartitionState[]{online, obsolete, upgrading};
    }

    public static MiruPartitionState[] rebuildingStates() {
        return new MiruPartitionState[]{rebuilding, upgrading};
    }
}
