package com.jivesoftware.os.miru.cluster;

/**
*
*/
public enum MiruTenantConfigFields {

    // explicit index is safer than ordinal and marshalls shorter than name
    //TODO consider compacting index to a byte
    number_of_replicas(0),
    topology_is_stale_after_millis(1);

    private final static MiruTenantConfigFields[] states;

    static {
        MiruTenantConfigFields[] values = values();
        int maxIndex = -1;
        for (MiruTenantConfigFields value : values) {
            maxIndex = Math.max(maxIndex, value.index);
        }
        states = new MiruTenantConfigFields[maxIndex + 1];
        for (MiruTenantConfigFields value : values) {
            states[value.index] = value;
        }
    }

    private final int index;

    private MiruTenantConfigFields(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    public static MiruTenantConfigFields fromIndex(int index) {
        return states[index];
    }
}
